import xlwings as xw
import time
import asyncio
import logging
import sys
from asyncua import Client, ua

# --- Configuration ---
# Replace with your server's endpoint URL
SERVER_URL = "opc.tcp://localhost:4840/simulator/server"

# Excel Configuration
EXCEL_FILE = './OPC_UA_Data.xlsx'
SHEET_NAME = 'Sheet1'
START_ROW = 2  # Start writing data from row 2 (assuming header in row 1)
# Column mappings (adjust as needed)
NODE_ID_COL = 1  # Column A
DISPLAY_NAME_COL = 2  # Column B
VALUE_COL = 3  # Column C
DATATYPE_COL = 4 # Column D

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
_logger = logging.getLogger('opcua_async_client_app')

node_cache = {}  # Stores asyncua.Node objects
node_display_names = {} # Stores display names (NodeId_str -> DisplayName)
node_excel_row = {} # Stores NodeId_str -> Excel Row
excel_row_to_node_id = {} # Stores Excel Row -> NodeId_str

# Excel Workbook and Sheet objects (global for easy access)
wb = None
sheet = None

# --- Excel Functions ---
def setup_excel():
    """Opens the Excel workbook and sets up headers if necessary."""
    global wb, sheet
    try:
        wb = xw.Book(EXCEL_FILE)
    except Exception:
        # If file doesn't exist, create it
        wb = xw.Book()
        wb.save(EXCEL_FILE)
        wb = xw.Book(EXCEL_FILE) # Reopen to get the actual workbook object

    sheet = wb.sheets[SHEET_NAME]

    # Write headers if the first row is empty
    if not sheet.range((1, NODE_ID_COL)).value:
        sheet.range((1, NODE_ID_COL)).value = "Node ID"
        sheet.range((1, DISPLAY_NAME_COL)).value = "Display Name"
        sheet.range((1, VALUE_COL)).value = "Value"
        sheet.range((1, DATATYPE_COL)).value = "Data Type"
        _logger.info("Excel headers written.")

    # Clear previous data (optional, but good for a fresh start)
    sheet.range(f'A{START_ROW}:Z1048576').clear_contents() # Clear all data below headers

def get_next_available_row():
    """Finds the next empty row in Excel for writing new data."""
    last_row = sheet.range((sheet.cells.last_cell.row, 1)).end('up').row
    return max(START_ROW, last_row + 1)

def write_data_to_excel(node_id_str, display_name, value, data_type):
    """Writes a single data point to Excel or updates an existing one."""
    global node_excel_row, excel_row_to_node_id

    row = node_excel_row.get(node_id_str)
    if row is None:
        # Node not yet in Excel, find next available row
        row = get_next_available_row()
        node_excel_row[node_id_str] = row
        excel_row_to_node_id[row] = node_id_str
        _logger.debug(f"Assigned new Excel row {row} to Node: {display_name} ({node_id_str})")

    # Write data to the specific cells
    sheet.range((row, NODE_ID_COL)).value = node_id_str
    sheet.range((row, DISPLAY_NAME_COL)).value = display_name
    sheet.range((row, VALUE_COL)).value = value
    sheet.range((row, DATATYPE_COL)).value = data_type
    # _logger.debug(f"Excel updated: {display_name} = {value}")

def read_data_from_excel_row(row):
    """Reads the value from a specific Excel row."""
    return sheet.range((row, VALUE_COL)).value

# --- Subscription Handler (Async) ---
class SubHandler(object):
    """
    Subscription Handler for asyncua. Callback methods must be async.
    """
    async def datachange_notification(self, node, val, data):
        """
        Called every time a subscribed node's value changes.
        Writes the change to Excel.
        """
        node_id_str = str(node.nodeid)
        display_name = node_display_names.get(node_id_str, "UNKNOWN_NODE")
        data_type = data.monitored_item.Value.Value.VariantType.name
        
        _logger.info(f"DATA CHANGE: Node '{display_name}' Value: {val} (DataType: {data_type})")
        
        # Write to Excel
        write_data_to_excel(node_id_str, display_name, val, data_type)


    async def event_notification(self, event):
        """
        Called when an event occurs (not used in this example, but good to have).
        """
        _logger.info(f"EVENT: {event}")

# --- Node Browsing and Subscription Logic (Async) ---
async def browse_and_subscribe_recursive(node, subscription, client, level=0):
    """
    Recursively browses nodes starting from 'node' and subscribes to data variables/properties.
    Caches all browsed nodes for later reference (e.g., writing).
    """
    indent = "  " * level
    
    try:
        children = await node.get_children()
    except Exception as e:
        _logger.warning(f"{indent}Could not browse children of '{await node.read_display_name()}' ({str(node.nodeid)}): {e}")
        return

    _logger.debug(f"{indent}Browsing: {await node.read_display_name()} ({str(node.nodeid)}) - Found {len(children)} children.")

    for child in children:
        child_display_name = "N/A"
        child_node_id_str = str(child.nodeid)

        try:
            child_display_name = (await child.read_display_name()).Text
            node_class = await child.read_node_class()
            
            # Cache node info for later use (e.g., writing)
            node_cache[child_node_id_str] = child
            node_display_names[child_node_id_str] = child_display_name

            # Skip nodes not in the server's default namespace (often 2 for simulator)
            # You might need to adjust this depending on your OPC UA server
            # if (child.nodeid.NamespaceIndex) != 2:
            #     _logger.debug(f"{indent}- Skipping node in non-default namespace: {child_display_name} ({child_node_id_str}) [NamespaceIndex: {child.nodeid.NamespaceIndex}]")
            #     continue

            if node_class == ua.NodeClass.Variable:
                _logger.info(f"{indent}- Subscribing to: {child_display_name} ({child_node_id_str}) [Type: {node_class.name}]")
                try:
                    await subscription.subscribe_data_change(child)
                    # Initialize Excel with current value
                    current_data_value = await child.read_data_value()
                    current_value = current_data_value.Value.Value
                    data_type_name = current_data_value.Value.VariantType.name
                    write_data_to_excel(child_node_id_str, child_display_name, current_value, data_type_name)
                except Exception as e:
                    _logger.warning(f"{indent}- Failed to subscribe to {child_display_name} ({child_node_id_str}): {e}")
            elif node_class == ua.NodeClass.Object:
                _logger.info(f"{indent}- Found Object: {child_display_name} ({child_node_id_str}). Browsing deeper...")
                await browse_and_subscribe_recursive(child, subscription, client, level + 1)
            elif node_class == ua.NodeClass.Method:
                _logger.debug(f"{indent}- Found Method: {child_display_name} ({child_node_id_str}). Skipping subscription.")
            else:
                _logger.debug(f"{indent}- Skipping node (not a Variable/Property/Object/Method): {child_display_name} ({child_node_id_str}) [Type: {node_class.name}]")

        except Exception as e:
            _logger.warning(f"{indent}- Could not process node '{child_display_name}' ({child_node_id_str}): {e}")

# --- Write Value Function (Async) ---
def _convert_input_to_opc_type(value_str, target_variant_type):
    try:
        if target_variant_type == ua.VariantType.Boolean:
            return value_str.lower() in ('true', '1', 't', 'y')
        elif target_variant_type in [ua.VariantType.Int16, ua.VariantType.Int32, ua.VariantType.Int64,
                                     ua.VariantType.SByte, ua.VariantType.Byte, ua.VariantType.UInt16,
                                     ua.VariantType.UInt32, ua.VariantType.UInt64]:
            return int(value_str)
        elif target_variant_type in [ua.VariantType.Float, ua.VariantType.Double]:
            return float(value_str)
        elif target_variant_type in [ua.VariantType.String, ua.VariantType.LocalizedText]:
            return str(value_str)
        else:
            _logger.warning(f"Attempting to write value '{value_str}' to unhandled VariantType: {target_variant_type.name}. Trying direct string conversion.")
            return value_str
    except ValueError:
        _logger.error(f"Could not convert '{value_str}' to target type {target_variant_type.name}. Please check input.")
        return None
    except Exception as e:
        _logger.error(f"An unexpected error occurred during type conversion for '{value_str}' to {target_variant_type.name}: {e}")
        return None

async def write_value_to_node_from_excel(client, row, node_id_str, new_value_from_excel):
    """
    Writes a value from Excel to the corresponding OPC UA node.
    """
    target_node = node_cache.get(node_id_str)
    
    if not target_node:
        _logger.error(f"Node with NodeId '{node_id_str}' not found in cache. Cannot write from Excel row {row}.")
        return

    try:
        current_data_value = await target_node.read_data_value()
        target_variant_type = current_data_value.Value.VariantType

        converted_value = _convert_input_to_opc_type(str(new_value_from_excel), target_variant_type)
        if converted_value is None:
            _logger.error(f"Value conversion failed for '{new_value_from_excel}' from Excel row {row}. Aborting write to OPC UA.")
            return

        variant_to_write = ua.Variant(converted_value, target_variant_type)
        
        await target_node.write_value(variant_to_write)
        _logger.info(f"Successfully wrote '{converted_value}' (from Excel row {row}) to OPC UA node '{(await target_node.read_display_name()).Text}' ({str(target_node.nodeid)})")

    except ua.UaError as e:
        _logger.error(f"OPC UA Error writing from Excel row {row} to node '{(await target_node.read_display_name()).Text}': {e}", exc_info=True)
    except Exception as e:
        _logger.error(f"An unexpected error occurred while writing from Excel row {row} to node '{(await target_node.read_display_name()).Text}': {e}", exc_info=True)

# --- Main Client Application (Async) ---
async def main():
    client = Client(SERVER_URL)
    subscription = None

    setup_excel()
    excel_last_read_values = {} # To track changes in Excel

    try:
        _logger.info(f"Attempting to connect to {SERVER_URL}...")
        async with client:
            _logger.info("Connection successful!")

            handler = SubHandler()
            subscription = await client.create_subscription(500, handler)

            _logger.info("Starting recursive browsing from 'Objects' folder and subscribing to data variables/properties...")
            objects_node = client.get_objects_node()
            await browse_and_subscribe_recursive(objects_node, subscription, client)
            
            _logger.info("Subscription setup complete. Monitoring data changes and Excel updates...")
            _logger.info("Keep the Excel file open. Changes in OPC UA will update Excel. Changes in Excel will update OPC UA.")

            # Initial read of all relevant Excel values to establish baseline
            for node_id_str, row in node_excel_row.items():
                value = read_data_from_excel_row(row)
                excel_last_read_values[row] = value

            # Loop to continuously monitor Excel for changes
            while True:
                # Iterate through all known Excel rows for subscribed nodes
                for row, node_id_str in excel_row_to_node_id.items():
                    current_excel_value = read_data_from_excel_row(row)
                    
                    # Check if the Excel value has changed since the last read
                    if row in excel_last_read_values and current_excel_value != excel_last_read_values[row]:
                        _logger.info(f"Excel change detected in row {row}: '{excel_last_read_values[row]}' -> '{current_excel_value}'")
                        await write_value_to_node_from_excel(client, row, node_id_str, current_excel_value)
                        excel_last_read_values[row] = current_excel_value # Update last read value

                await asyncio.sleep(1) # Check Excel every second

    except ConnectionRefusedError:
        _logger.error(f"Connection refused. Is the server running at {SERVER_URL}?")
    except ua.UaError as e:
        _logger.error(f"OPC UA Error: {e.local_description.Text} (Code: {e.code.name})", exc_info=True)
        print(f"OPC UA Error: {e.local_description.Text} (Code: {e.code.name})")
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
        
        # Ensure Excel workbook is closed gracefully if it was opened by xlwings
        if wb:
            try:
                wb.save() # Save any changes made by the script
                wb.close()
                _logger.info("Excel workbook closed.")
            except Exception as e:
                _logger.warning(f"Error closing Excel workbook: {e}")

        _logger.info("Client application finished.")

if __name__ == "__main__":
    asyncio.run(main())