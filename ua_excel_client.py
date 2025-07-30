import xlwings as xw
import time
import asyncio
import logging
import sys
import os # <-- ADD THIS IMPORT
from asyncua import Client, ua

# --- Configuration ---
# Replace with your server's endpoint URL
# SERVER_URL = "opc.tcp://localhost:4840/simulator/server" # <-- REMOVE OR COMMENT OUT THIS LINE

# New: Configuration file name
CONFIG_FILE = 'config.txt'

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

node_cache = {}  # Stores asyncua.Node objects (NodeId_str -> Node_obj)
node_display_names = {} # Stores display names (NodeId_str -> DisplayName_str)
node_excel_row = {} # Stores NodeId_str -> Excel Row_int
excel_row_to_node_id = {} # Stores Excel Row_int -> NodeId_str

# Excel Workbook and Sheet objects (global for easy access)
wb = None
sheet = None

# Variable to hold the active subscription object
subscription = None

# This dictionary will store the last known value from Excel for each row
# Used to detect manual changes by the user in Excel
excel_last_read_values = {}

# --- New Function to Load Configuration ---
def load_config(config_file_name):
    """
    Loads configuration from a specified file.
    Expects key=value pairs, one per line. Skips comments (#) and empty lines.
    """
    config = {}
    # Construct the full path to the config file relative to the script
    script_dir = os.path.dirname(__file__)
    config_path = os.path.join(script_dir, config_file_name)

    if not os.path.exists(config_path):
        _logger.error(f"Error: Configuration file '{config_path}' not found.")
        _logger.error("Please create a 'config.txt' file in the same directory as 'ua_excel_client.py'.")
        _logger.error("It should contain 'SERVER_URL=opc.tcp://your_server_address:port/path' (e.g., SERVER_URL=opc.tcp://localhost:4840/simulator/server).")
        sys.exit(1) # Exit if the config file is missing

    try:
        with open(config_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'): # Skip empty lines and comments
                    continue
                if '=' in line:
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()
    except Exception as e:
        _logger.error(f"Error reading configuration file '{config_path}': {e}")
        sys.exit(1) # Exit if there's an error reading the config file

    return config


# --- Excel Functions ---
def setup_excel():
    """Opens the Excel workbook and sets up headers if necessary."""
    global wb, sheet
    try:
        # Try to open existing workbook
        wb = xw.Book(EXCEL_FILE)
    except Exception:
        # If file doesn't exist, create it and then reopen
        _logger.info(f"Excel file '{EXCEL_FILE}' not found. Creating a new one.")
        wb = xw.Book()
        wb.save(EXCEL_FILE)
        wb = xw.Book(EXCEL_FILE) # Reopen to ensure it's loaded correctly

    sheet = wb.sheets[SHEET_NAME]

    # Write headers if the first row is empty
    if not sheet.range((1, NODE_ID_COL)).value:
        sheet.range((1, NODE_ID_COL)).value = "Node ID"
        sheet.range((1, DISPLAY_NAME_COL)).value = "Display Name"
        sheet.range((1, VALUE_COL)).value = "Value"
        sheet.range((1, DATATYPE_COL)).value = "Data Type"
        _logger.info("Excel headers written.")

    # Clear previous data (good for a fresh start, especially after a re-scan)
    # Clear from START_ROW downwards
    last_col = max(NODE_ID_COL, DISPLAY_NAME_COL, VALUE_COL, DATATYPE_COL)
    sheet.range((START_ROW, 1), (sheet.cells.last_cell.row, last_col)).clear_contents()
    _logger.info("Cleared existing data in Excel (below headers).")


def get_next_available_row():
    """Finds the next empty row in Excel for writing new data."""
    last_used_row = sheet.range((sheet.cells.last_cell.row, NODE_ID_COL)).end('up').row
    return max(START_ROW, last_used_row + 1)

def write_data_to_excel(node_id_str, display_name, value, data_type):
    """Writes a single data point to Excel or updates an existing one."""
    global node_excel_row, excel_row_to_node_id, excel_last_read_values

    row = node_excel_row.get(node_id_str)
    if row is None:
        # Node not yet in Excel, find next available row
        row = get_next_available_row()
        node_excel_row[node_id_str] = row
        excel_row_to_node_id[row] = node_id_str
        _logger.debug(f"Assigned new Excel row {row} to Node: ({node_id_str})")

    # Write data to the specific cells
    # Use tuple for range(row, column) to be precise
    sheet.range((row, NODE_ID_COL)).value = node_id_str
    sheet.range((row, DISPLAY_NAME_COL)).value = display_name
    sheet.range((row, VALUE_COL)).value = value
    sheet.range((row, DATATYPE_COL)).value = data_type
    
    # IMPORTANT FIX: Update excel_last_read_values when we write from OPC UA
    # This prevents the change from OPC UA from immediately triggering an Excel->OPC UA write back
    excel_last_read_values[row] = value 


def read_data_from_excel_row(row):
    """Reads the value from a specific Excel row's Value column."""
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
async def browse_and_subscribe_recursive(node, subscription_obj, client, level=0):
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

    for child in children:
        child_display_name = "N/A"
        child_node_id_str = str(child.nodeid)

        try:
            child_display_name, node_class = await child.read_attributes([ua.AttributeIds.DisplayName, ua.AttributeIds.NodeClass])
            child_display_name = child_display_name.Value.Value.Text
            node_class = ua.NodeClass(node_class.Value.Value)
            
            node_cache[child_node_id_str] = child
            node_display_names[child_node_id_str] = child_display_name

            if node_class == ua.NodeClass.Variable:
                try:
                    await subscription_obj.subscribe_data_change(child)
                except Exception as e:
                    _logger.warning(f"{indent}- Failed to subscribe to {child_display_name} ({child_node_id_str}): {e}")
            elif node_class == ua.NodeClass.Object:
                await browse_and_subscribe_recursive(child, subscription_obj, client, level + 1)
            
        except Exception as e:
            _logger.warning(f"{indent}- Could not process node '{child_display_name}' ({child_node_id_str}): {e}")

# --- Write Value Function (Async) ---
def _convert_input_to_opc_type(value_str, target_variant_type):
    """Converts a string value from Excel into the appropriate Python type for OPC UA."""
    try:
        # Handle None/empty string explicitly if Excel cell is blank
        if value_str is None or str(value_str).strip() == '':
            _logger.debug(f"Input value for conversion is empty/None. Returning default for {target_variant_type.name}.")
            # Return a default/zero equivalent for the type, or None if you prefer
            if target_variant_type == ua.VariantType.Boolean: return False
            if target_variant_type in [ua.VariantType.Int16, ua.VariantType.Int32, ua.VariantType.Int64, ua.VariantType.SByte, ua.VariantType.Byte, ua.VariantType.UInt16, ua.VariantType.UInt32, ua.VariantType.UInt64]: return 0
            if target_variant_type in [ua.VariantType.Float, ua.VariantType.Double]: return 0.0
            if target_variant_type in [ua.VariantType.String, ua.VariantType.LocalizedText]: return ""
            return None # Fallback for unhandled types

        # Actual conversion logic
        if target_variant_type == ua.VariantType.Boolean:
            return str(value_str).lower() in ('true', '1', 't', 'y', 'yes', 'on')
        elif target_variant_type in [ua.VariantType.Int16, ua.VariantType.Int32, ua.VariantType.Int64,
                                     ua.VariantType.SByte, ua.VariantType.Byte, ua.VariantType.UInt16,
                                     ua.VariantType.UInt32, ua.VariantType.UInt64]:
            return int(float(value_str)) # Use float() first to handle "1.0" for ints
        elif target_variant_type in [ua.VariantType.Float, ua.VariantType.Double]:
            return float(value_str)
        elif target_variant_type in [ua.VariantType.String, ua.VariantType.LocalizedText]:
            return str(value_str)
        # Add more types as needed (e.g., DateTime, Guid, etc.)
        else:
            _logger.warning(f"Attempting to write value '{value_str}' to unhandled VariantType: {target_variant_type.name}. Trying direct string conversion.")
            return str(value_str) # Fallback: return as string, let set_value try to convert
    except (ValueError, TypeError): # Catch both conversion errors and NoneType errors
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
        _logger.error(f"Node with NodeId '{node_id_str}' not found in cache. Cannot write from Excel row {row}. (Perhaps node was deleted from server?)")
        return

    try:
        # Get the current data value to infer the target variant type
        # This is important as we want to write the correct OPC UA type
        current_data_value = await target_node.read_data_value()
        target_variant_type = current_data_value.Value.VariantType

        converted_value = _convert_input_to_opc_type(new_value_from_excel, target_variant_type)
        if converted_value is None and new_value_from_excel is not None and str(new_value_from_excel).strip() != '':
            # Only log error if conversion failed and input was not truly empty
            _logger.error(f"Value conversion failed for '{new_value_from_excel}' from Excel row {row}. Aborting write to OPC UA.")
            return

        # Create a Variant object with the correct type
        # If converted_value is None because _convert_input_to_opc_type returned None (e.g., empty string for non-string type)
        # we still create a Variant, as asyncua's write_value can handle None for some types or attempt conversion.
        variant_to_write = ua.Variant(converted_value, target_variant_type)
        
        await target_node.write_value(variant_to_write)
        _logger.info(f"Successfully wrote '{converted_value}' (from Excel row {row}) to OPC UA node '{(await target_node.read_display_name()).Text}' ({str(target_node.nodeid)})")

    except ua.UaError as e:
        _logger.error(f"OPC UA Error writing from Excel row {row} to node '{(await target_node.read_display_name()).Text}': {e}", exc_info=True)
    except Exception as e:
        _logger.error(f"An unexpected error occurred while writing from Excel row {row} to node '{(await target_node.read_display_name()).Text}': {e}", exc_info=True)


async def rescan_opc_ua_nodes(client, sub_handler):
    """
    Deletes existing subscriptions, clears caches and Excel,
    then re-browses the server and re-subscribes.
    """
    global subscription 
    global excel_last_read_values 

    _logger.info("Initiating full OPC UA node re-scan...")

    # 1. Delete existing subscription if any
    if subscription:
        try:
            _logger.info("Deleting existing subscription...")
            await subscription.delete()
            _logger.info("Subscription deleted.")
        except Exception as e:
            _logger.warning(f"Error deleting old subscription during rescan: {e}")
        finally:
            subscription = None 

    # 2. Clear all cached node information
    node_cache.clear()
    node_display_names.clear()
    node_excel_row.clear()
    excel_row_to_node_id.clear()
    excel_last_read_values.clear() 

    # 3. Clear Excel data (important for a clean sync)
    setup_excel() # This also clears below headers

    # 4. Create a new subscription
    try:
        subscription = await client.create_subscription(500, sub_handler)
        _logger.info("New subscription created for re-scan.")
    except Exception as e:
        _logger.error(f"Failed to create new subscription during rescan: {e}")
        return 

    # 5. Re-browse server and re-subscribe to data variables
    _logger.info("Re-browsing server and re-subscribing to data variables...")
    objects_node = client.get_objects_node()
    start_time = time.time()  
    await browse_and_subscribe_recursive(objects_node, subscription, client)
    end_time = time.time()  
    _logger.info(f"Re-scan completed in {end_time - start_time:.2f} seconds.")
    _logger.info("OPC UA node re-scan complete.")

    # Re-populate excel_last_read_values after re-scan
    # This loop is crucial because initial `write_data_to_excel` populates `node_excel_row`
    # and `write_data_to_excel` also updates `excel_last_read_values`
    # We do a final loop here just to ensure everything is consistent
    # (though `write_data_to_excel` should have set these already)
    for node_id_str, row in node_excel_row.items():
        value = read_data_from_excel_row(row)
        excel_last_read_values[row] = value
    _logger.info("Excel baseline values re-established after re-scan.")


# --- Main Client Application (Async) ---
async def main():
    global subscription 

    # --- Load Configuration ---
    app_config = load_config(CONFIG_FILE)
    server_url = app_config.get("SERVER_URL")

    if not server_url:
        _logger.error("SERVER_URL not found in config.txt. Please ensure it's defined (e.g., SERVER_URL=opc.tcp://localhost:4840/simulator/server).")
        sys.exit(1) # Exit if SERVER_URL is not found in the config

    client = Client(server_url) # Use the loaded server_url
    
    try:
        _logger.info(f"Attempting to connect to {server_url} (from {CONFIG_FILE})...") # Log the source
        async with client: 
            _logger.info("Connection successful!")

            handler = SubHandler()
            
            # Perform initial setup and scan
            await rescan_opc_ua_nodes(client, handler)
            
            _logger.info("Monitoring data changes from OPC UA to Excel and vice-versa.")
            _logger.info("Type 'r' to rescan OPC UA nodes, 'q' to quit.")

            # Run a separate task to handle user input (r/q)
            input_task = asyncio.create_task(read_user_input())

            while True:
                # Use asyncio.wait with a timeout to allow for periodic Excel checks
                # and to react to user input immediately
                done, pending = await asyncio.wait([input_task], return_when=asyncio.FIRST_COMPLETED, timeout=0.5) 
                
                # If input_task is done, process its result
                for task in done:
                    user_input = task.result().strip().lower()
                    if user_input == 'q':
                        _logger.info("Quit command received. Exiting.")
                        return 
                    elif user_input == 'r':
                        await rescan_opc_ua_nodes(client, handler) 
                        input_task = asyncio.create_task(read_user_input()) 
                    else:
                        _logger.info("Unknown command. Type 'r' to rescan, 'q' to quit.")
                        input_task = asyncio.create_task(read_user_input()) 

                # This block runs periodically (after timeout if no input)
                # Check for changes in Excel and write to OPC UA
                for row, node_id_str in list(excel_row_to_node_id.items()):
                    try:
                        current_excel_value = read_data_from_excel_row(row)
                        
                        # Check if the Excel value has changed since the last read
                        if current_excel_value != excel_last_read_values.get(row):
                            _logger.info(f"Excel detected change in row {row} (NodeId: {node_id_str}): old='{excel_last_read_values.get(row)}', new='{current_excel_value}'")
                            
                            await write_value_to_node_from_excel(client, row, node_id_str, current_excel_value)
                            excel_last_read_values[row] = current_excel_value 

                    except Exception as e:
                        _logger.error(f"Error processing Excel row {row} (NodeId: {node_id_str}): {e}", exc_info=True)
                        pass 

    except ConnectionRefusedError:
        _logger.error(f"Connection refused. Is the server running at {server_url}?")
    except ua.UaError as e:
        _logger.error(f"OPC UA Error: {e.local_description.Text} (Code: {e.code.name})", exc_info=True)
        print(f"OPC UA Error: {e.local_description.Text} (Code: {e.code.name})")
    except Exception as e:
        _logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        # Final cleanup for subscription and Excel
        if subscription:
            _logger.info("Deleting subscription...")
            try:
                await subscription.delete()
                _logger.info("Subscription deleted.")
            except Exception as e:
                _logger.warning(f"Error deleting final subscription: {e}")
        
        if wb:
            try:
                wb.save() 
                wb.close()
                _logger.info("Excel workbook saved and closed.")
            except Exception as e:
                _logger.warning(f"Error saving/closing Excel workbook: {e}")

        _logger.info("Client application finished.")

async def read_user_input():
    """Reads a line from stdin in an async-compatible way."""
    return await asyncio.to_thread(sys.stdin.readline)

if __name__ == "__main__":
    asyncio.run(main())