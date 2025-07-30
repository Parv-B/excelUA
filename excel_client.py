import asyncio
import logging
import sys
import xlwings as xw
from asyncua import Client, ua
import threading 

# --- Configuration ---
SERVER_URL = "opc.tcp://localhost:4840/simulator/server"
EXCEL_FILE_PATH = 'Book1.xlsx'
EXCEL_SHEET_NAME = 'Sheet1'
# Excel column mappings for OPC UA data
NODE_DISPLAY_NAME_COL = 1 # Column A
NODE_ID_COL = 2           # Column B
CURRENT_VALUE_COL = 3     # Column C
WRITE_VALUE_COL = 4       # Column D
START_ROW_DATA = 2        # Data starts from row 2

# NEW: Namespace to filter by
TARGET_NAMESPACE_INDEX = 2

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='opcua_async_excel_client.log')
_logger = logging.getLogger('opcua_async_excel_client_app')

# Global storage for OPC UA nodes and their Excel row mapping
# {node_id_str: {'node_obj': Node, 'display_name': str, 'excel_row': int, 'last_excel_write_value': any}}
opc_nodes_in_excel = {}

excel_app = None

# --- Subscription Handler (Async) ---
class SubHandler(object):
    """
    Subscription Handler for asyncua. Callback methods must be async.
    """
    async def datachange_notification(self, node, val, data):
        """
        Called every time a subscribed node's value changes.
        """
        node_id_str = str(node.nodeid)
        display_name = opc_nodes_in_excel.get(node_id_str, {}).get('display_name', "UNKNOWN_NODE")
        _logger.info(f"DATA CHANGE: Node '{display_name}' Value: {val} (DataType: {(data.monitored_item.Value.Value.VariantType)})")

        # Update Excel for this specific node's current value
        if node_id_str in opc_nodes_in_excel:
            excel_row = opc_nodes_in_excel[node_id_str]['excel_row']
            # Run Excel update in a separate thread to not block async loop
            await asyncio.to_thread(self._update_excel_cell_sync, excel_row, CURRENT_VALUE_COL, val)
    
    async def event_notification(self, event):
        """
        Called when an event occurs (not used in this example, but good to have).
        """
        _logger.info(f"EVENT: {event}")

    def _update_excel_cell_sync(self, row, col, value):
        """Synchronous Excel update function to be run in a separate thread."""
        try:
            # Ensure we're interacting with the *correct* workbook
            book = xw.apps.active.books[EXCEL_FILE_PATH.split('/')[-1]] # Get by filename
            sheet = book.sheets[EXCEL_SHEET_NAME]
            
            cell_range = sheet.cells(row, col)
            current_excel_value = cell_range.value
            
            # Only update if value has changed to reduce Excel redraws
            # Compare as strings for robustness with types like None/int/float
            if str(current_excel_value) != str(value): 
                cell_range.value = value
                _logger.debug(f"Excel updated: R{row}C{col} = {value}")
            
        except Exception as e:
            _logger.error(f"Error updating Excel cell R{row}C{col}: {e}")

# --- Node Browsing and Subscription Logic (Async) ---
async def browse_and_subscribe_recursive(node, subscription, client, current_excel_row, level=0):
    """
    Recursively browses nodes and populates Excel, filtering by TARGET_NAMESPACE_INDEX.
    Returns the next available Excel row.
    """
    indent = "  " * level
    
    try:
        children = await node.get_children()
    except Exception as e:
        _logger.warning(f"{indent}Could not browse children of '{await node.read_display_name()}' ({str(node.nodeid)}): {e}")
        return current_excel_row

    for child in children:
        child_display_name_text = "N/A"
        child_node_id_str = str(child.nodeid)

        if child.nodeid.NamespaceIndex != TARGET_NAMESPACE_INDEX:
            _logger.debug(f"{indent}- Skipping node '{child_node_id_str}' (Namespace: {child.nodeid.NamespaceIndex}, not {TARGET_NAMESPACE_INDEX})")
            continue # Skip this node, don't process it further

        try:
            child_display_name_text = (await child.read_display_name()).Text
            node_class = await child.read_node_class()
            
            if node_class == ua.NodeClass.Variable:
                _logger.info(f"{indent}- Subscribing to: {child_display_name_text} ({child_node_id_str}) [Type: {node_class.name}]")
                try:
                    await subscription.subscribe_data_change(child)
                    
                    # Store node info and its Excel row
                    opc_nodes_in_excel[child_node_id_str] = {
                        'node_obj': child,
                        'display_name': child_display_name_text,
                        'excel_row': current_excel_row,
                        'last_excel_write_value': None # Track the last value written from Excel
                    }

                    # Populate Excel with Node Display Name and Node ID
                    await asyncio.to_thread(
                        _populate_excel_node_info_sync, 
                        current_excel_row, 
                        child_display_name_text, 
                        child_node_id_str
                    )
                    
                    current_excel_row += 1 # Move to the next row for the next node
                except Exception as e:
                    _logger.warning(f"{indent}- Failed to subscribe to {child_display_name_text} ({child_node_id_str}): {e}")
            elif node_class == ua.NodeClass.Object:
                _logger.info(f"{indent}- Found Object: {child_display_name_text} ({child_node_id_str}). Browsing deeper...")
                current_excel_row = await browse_and_subscribe_recursive(child, subscription, client, current_excel_row, level + 1)
            elif node_class == ua.NodeClass.Method:
                _logger.debug(f"{indent}- Found Method: {child_display_name_text} ({child_node_id_str}). Skipping subscription.")
            else:
                _logger.debug(f"{indent}- Skipping node (not a Variable/Property/Object/Method): {child_display_name_text} ({child_node_id_str}) [Type: {node_class.name}]")

        except Exception as e:
            _logger.warning(f"{indent}- Could not process node '{child_display_name_text}' ({child_node_id_str}): {e}")
            
    return current_excel_row

def _populate_excel_node_info_sync(row, display_name, node_id_str):
    """Synchronous function to populate Excel with node info."""
    try:
        book = xw.apps.active.books[EXCEL_FILE_PATH.split('/')[-1]]
        sheet = book.sheets[EXCEL_SHEET_NAME]
        sheet.cells(row, NODE_DISPLAY_NAME_COL).value = display_name
        sheet.cells(row, NODE_ID_COL).value = node_id_str
        _logger.debug(f"Excel populated: R{row}C{NODE_DISPLAY_NAME_COL}={display_name}, R{row}C{NODE_ID_COL}={node_id_str}")
    except Exception as e:
        _logger.error(f"Error populating Excel node info R{row}: {e}")

def _convert_input_to_opc_type(value_str, target_variant_type):
    """Helper to convert string input to OPC UA VariantType."""
    try:
        if value_str is None or str(value_str).strip() == "": # Handle empty string and None
            return None # Allow empty string to signal no value / clear
        
        # Convert value_str to a string consistently before operations
        value_str = str(value_str).strip()

        if target_variant_type == ua.VariantType.Boolean:
            return value_str.lower() in ('true', '1', 't', 'y')
        elif target_variant_type in [ua.VariantType.Int16, ua.VariantType.Int32, ua.VariantType.Int64,
                                     ua.VariantType.SByte, ua.VariantType.Byte, ua.VariantType.UInt16,
                                     ua.VariantType.UInt32, ua.VariantType.UInt64]:
            return int(value_str)
        elif target_variant_type in [ua.VariantType.Float, ua.VariantType.Double]:
            return float(value_str)
        elif target_variant_type in [ua.VariantType.String, ua.VariantType.LocalizedText]:
            return value_str
        # Add more types as needed (e.g., DateTime, Guid, etc.)
        else:
            _logger.warning(f"Attempting to convert '{value_str}' to unhandled VariantType: {target_variant_type.name}. Trying direct string conversion.")
            return value_str # Fallback
    except ValueError:
        _logger.error(f"Could not convert '{value_str}' to target type {target_variant_type.name}. Check Excel input.")
        return None
    except Exception as e:
        _logger.error(f"An unexpected error occurred during type conversion for '{value_str}' to {target_variant_type.name}: {e}")
        return None

async def process_excel_writes_and_read(client):
    """
    Reads the 'Write Value' column in Excel synchronously, and if changes are detected,
    triggers asynchronous OPC UA writes.
    This function itself is asynchronous because it calls other awaitable functions.
    """
    global opc_nodes_in_excel

    try:
        # Use asyncio.to_thread to run the synchronous Excel read operation
        excel_read_data = await asyncio.to_thread(_read_excel_write_column_sync)

        # Iterate through the data read from Excel and trigger OPC UA writes if needed
        for node_id_str, excel_write_value in excel_read_data.items():
            if node_id_str in opc_nodes_in_excel:
                node_info = opc_nodes_in_excel[node_id_str]

                # Compare with the last value we processed from Excel to avoid redundant writes
                # Use string conversion for robust comparison between Excel's None, "" and Python's None
                if str(excel_write_value) != str(node_info['last_excel_write_value']):
                    _logger.info(f"Detected Excel write request for '{node_info['display_name']}': '{excel_write_value}'")
                    
                    target_node_obj = node_info['node_obj']
                    try:
                        # Determine target VariantType from the OPC UA node itself
                        current_data_value = await target_node_obj.read_data_value()
                        target_variant_type = current_data_value.Value.VariantType
                        
                        converted_value = _convert_input_to_opc_type(excel_write_value, target_variant_type)
                        
                        if converted_value is not None:
                            variant_to_write = ua.Variant(converted_value, target_variant_type)
                            await target_node_obj.write_value(variant_to_write)
                            _logger.info(f"Successfully wrote '{converted_value}' (Type: {target_variant_type.name}) to OPC UA node '{node_info['display_name']}'.")
                            # Update last_excel_write_value to prevent immediate re-write
                            node_info['last_excel_write_value'] = excel_write_value 
                        else:
                            _logger.warning(f"Skipping write to '{node_info['display_name']}' due to type conversion failure for value '{excel_write_value}'.")
                            # Don't update last_excel_write_value so user can try again
                    except ua.UaError as e:
                        _logger.error(f"OPC UA Error writing to node '{node_info['display_name']}': {e}")
                        # Don't update last_excel_write_value so user can try again
                    except Exception as e:
                        _logger.error(f"An unexpected error occurred while writing to node '{node_info['display_name']}': {e}")
                        # Don't update last_excel_write_value so user can try again
                elif excel_write_value is None and node_info['last_excel_write_value'] is not None:
                    # If Excel cell is cleared, also clear our tracking state
                    node_info['last_excel_write_value'] = None

    except Exception as e:
        _logger.error(f"Error during Excel write processing: {e}")

def _read_excel_write_column_sync():
    """Synchronously reads the 'Write Value' column from Excel."""
    write_values = {}
    try:
        book = xw.apps.active.books[EXCEL_FILE_PATH.split('/')[-1]]
        sheet = book.sheets[EXCEL_SHEET_NAME]

        # Get the maximum row that has data in the browsed nodes section
        max_row = START_ROW_DATA + len(opc_nodes_in_excel) -1
        if max_row < START_ROW_DATA: # Handle case with no nodes
            return {}

        # Read range as a list of lists, then flatten to list of values
        # This will be [ [val_row2], [val_row3], ... ]
        values_from_excel = sheet.range(START_ROW_DATA, WRITE_VALUE_COL).expand('down').value

        if values_from_excel is None: # Column is empty
            return {}
        
        # Ensure values_from_excel is a list (even if single value)
        if not isinstance(values_from_excel, list):
            values_from_excel = [values_from_excel]
        elif not values_from_excel or not isinstance(values_from_excel[0], list): # handle cases like [[val]] vs [val]
             values_from_excel = [[v] for v in values_from_excel] # Ensure it's list of lists

        # Map Excel values back to NodeIds
        # This requires iterating through opc_nodes_in_excel to find the correct row
        for node_id_str, node_info in opc_nodes_in_excel.items():
            row_offset = node_info['excel_row'] - START_ROW_DATA
            if 0 <= row_offset < len(values_from_excel):
                write_values[node_id_str] = values_from_excel[row_offset][0] # Assuming single column read
        
    except Exception as e:
        _logger.error(f"Error reading Excel write column: {e}")
    return write_values


# --- Excel Initialization (Synchronous) ---
def init_excel():
    """
    Initializes xlwings, opens the workbook, and clears previous data.
    This must run *before* the asyncio loop starts.
    """
    global excel_app

    try:
        excel_app = xw.App(visible=True, add_book=False) 
        _logger.info(f"Excel application started (PID: {excel_app.pid}).")
        
        excel_wb = excel_app.books.open(EXCEL_FILE_PATH)
        _logger.info(f"Opened Excel workbook: {EXCEL_FILE_PATH}")

        excel_sheet = excel_wb.sheets[EXCEL_SHEET_NAME]
        
        excel_sheet.range(f'A{START_ROW_DATA}:D1000').clear_contents() # Clear up to row 1000
        _logger.info(f"Cleared previous data in '{EXCEL_SHEET_NAME}' from row {START_ROW_DATA}.")

        excel_sheet.cells(1, NODE_DISPLAY_NAME_COL).value = "Node Display Name"
        excel_sheet.cells(1, NODE_ID_COL).value = "Node ID"
        excel_sheet.cells(1, CURRENT_VALUE_COL).value = "Current Value"
        excel_sheet.cells(1, WRITE_VALUE_COL).value = "Write Value"
        excel_sheet.autofit() # Autofit columns

        _logger.info("Excel setup complete.")
        return True
    except Exception as e:
        _logger.error(f"Failed to initialize Excel: {e}", exc_info=True)
        return False

def close_excel():
    """Closes the Excel workbook and application."""
    global excel_app
    if excel_app and excel_app.books:
        try:
            book_filename = EXCEL_FILE_PATH.split('/')[-1]
            if book_filename in excel_app.books:
                excel_book_to_close = excel_app.books[book_filename]
                if not excel_book_to_close.saved:
                    excel_book_to_close.save()
                    _logger.info(f"Excel workbook '{book_filename}' saved.")
                excel_book_to_close.close()
                _logger.info(f"Excel workbook '{book_filename}' closed.")
            else:
                _logger.info(f"Workbook '{book_filename}' not found in active Excel application. Skipping close.")

        except Exception as e:
            _logger.warning(f"Error closing Excel workbook: {e}")
    
    if excel_app:
        try:
            _logger.info("Attempting to quit Excel application...")
            excel_app.quit()
            _logger.info("Excel application quit.")
        except Exception as e:
            _logger.warning(f"Error quitting Excel application: {e}")

# --- Main Client Application (Async) ---
async def main():
    if not init_excel():
        _logger.critical("Excel initialization failed. Exiting.")
        return

    client = Client(SERVER_URL)
    subscription = None
    try:
        _logger.info(f"Attempting to connect to {SERVER_URL}...")
        async with client:
            _logger.info("Connection successful!")

            handler = SubHandler()
            subscription = await client.create_subscription(500, handler)

            _logger.info(f"Starting recursive browsing from 'Objects' folder and populating Excel (filtering for Namespace: {TARGET_NAMESPACE_INDEX})...")
            objects_node = client.get_objects_node()
            next_excel_row = await browse_and_subscribe_recursive(objects_node, subscription, client, START_ROW_DATA)
            _logger.info(f"Finished browsing. Populated {next_excel_row - START_ROW_DATA} nodes in Excel (Namespace {TARGET_NAMESPACE_INDEX}).")
            
            _logger.info("Subscription setup complete. Monitoring data changes and Excel for write requests...")
            _logger.info("Type 'q' to quit.")

            while True:
                await process_excel_writes_and_read(client)
                
                try:
                    user_input_task = asyncio.create_task(asyncio.to_thread(lambda: sys.stdin.readline().strip().lower() if sys.stdin.isatty() else ''))
                    done, pending = await asyncio.wait([user_input_task], timeout=1.0)
                    if done:
                        user_input = user_input_task.result()
                        if user_input == 'q':
                            _logger.info("Quit command received. Exiting.")
                            break
                        elif user_input:
                            _logger.info(f"Unrecognized console command: '{user_input}'. Type 'q' to quit.")
                except Exception as e:
                    _logger.debug(f"Could not read console input: {e}. Running without console commands.")
                    await asyncio.sleep(1.0)
                
    except ConnectionRefusedError:
        _logger.error(f"Connection refused. Is the server running at {SERVER_URL}?")
    except ua.UaError as e:
        _logger.error(f"OPC UA Error: {e.local_description.Text} (Code: {e.code.name})", exc_info=True)
    except Exception as e:
        _logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if subscription:
            _logger.info("Deleting subscription...")
            try:
                await subscription.delete()
                _logger.info("Subscription deleted.")
            except Exception as e:
                _logger.warning(f"Error deleting subscription: {e}")
        _logger.info("Client application finished.")
        close_excel()

if __name__ == "__main__":
    asyncio.run(main())