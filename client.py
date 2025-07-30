import asyncio
import logging
import sys
from asyncua import Client, ua

# --- Configuration ---
# Replace with your server's endpoint URL
SERVER_URL = "opc.tcp://localhost:4840/simulator/server"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
_logger = logging.getLogger('opcua_async_client_app')

node_cache = {}
node_display_names = {}

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
        display_name = node_display_names.get(node_id_str, "UNKNOWN_NODE")
        _logger.info(f"DATA CHANGE: Node '{display_name}' Value: {val} (DataType: {(data.monitored_item.Value.Value.VariantType)})")

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
        children = await node.get_children() # Await the get_children call
    except Exception as e:
        _logger.warning(f"{indent}Could not browse children of '{await node.read_display_name()}' ({str(node.nodeid)}): {e}") # FIX
        return

    _logger.debug(f"{indent}Browsing: {await node.read_display_name()} ({str(node.nodeid)}) - Found {len(children)} children.") # FIX

    for child in children:
        child_display_name = "N/A"
        # FIX: Use str(child.nodeid) directly
        child_node_id_str = str(child.nodeid)

        try:
            # Await node property reads
            child_display_name = (await child.read_display_name()).Text
            node_class = await child.read_node_class()
            
            # Cache node info for later use (e.g., writing)
            node_cache[child_node_id_str] = child
            node_display_names[child_node_id_str] = child_display_name

            
            print(ua.NodeClass.Variable, ua.NodeClass.Object, ua.NodeClass.Method)
            

            if node_class == ua.NodeClass.Variable:
                _logger.info(f"{indent}- Subscribing to: {child_display_name} ({child_node_id_str}) [Type: {node_class.name}]")
                try:
                    await subscription.subscribe_data_change(child) # Await subscription
                except Exception as e:
                    _logger.warning(f"{indent}- Failed to subscribe to {child_display_name} ({child_node_id_str}): {e}")
            elif node_class == ua.NodeClass.Object:
                _logger.info(f"{indent}- Found Object: {child_display_name} ({child_node_id_str}). Browsing deeper...")
                await browse_and_subscribe_recursive(child, subscription, client, level + 1) # Await recursive call
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
        # Add more types as needed (e.g., DateTime, Guid, etc.)
        else:
            _logger.warning(f"Attempting to write value '{value_str}' to unhandled VariantType: {target_variant_type.name}. Trying direct string conversion.")
            return value_str # Fallback: return as string, let set_value try to convert
    except ValueError:
        _logger.error(f"Could not convert '{value_str}' to target type {target_variant_type.name}. Please check input.")
        return None
    except Exception as e:
        _logger.error(f"An unexpected error occurred during type conversion for '{value_str}' to {target_variant_type.name}: {e}")
        return None

async def write_value_to_node(client):
    """
    Prompts the user for a node ID/path and a value to write.
    This function is an async wrapper for user interaction.
    """
    print("\n--- Write Value ---")
    print("Available nodes for writing (by display name or NodeId):")
    
    if not node_cache:
        print("  No nodes have been discovered yet. Connect and browse first!")
        return
    
    # Display cached nodes for user reference, sorting by display name
    sorted_display_names = sorted(node_display_names.items(), key=lambda item: item[1])
    for node_id_str, display_name in sorted_display_names:
        print(f"  - {display_name} (NodeId: {node_id_str})")

    # Use input() directly as this is a blocking user prompt within an async context
    # It's generally fine for interactive prompts that are not performance-critical.
    node_identifier = input("Enter Node Display Name or NodeId (e.g., 'MyVariable' or 'ns=2;i=3'): ").strip()
    if not node_identifier:
        print("No node identifier entered. Aborting write operation.")
        return

    # Try to find the node by display name first, then by NodeId string
    target_node = None
    # Check by display name
    for node_id_str, display_name in node_display_names.items():
        if display_name == node_identifier:
            target_node = node_cache.get(node_id_str)
            break
    
    # If not found by display name, try by direct NodeId string from cache
    if not target_node:
        target_node = node_cache.get(node_identifier) 
    
    # If still not found, try client.get_node which can parse various NodeId formats directly
    if not target_node:
        try:
            target_node = await client.get_node(node_identifier) # Await get_node
            # Add to cache if successfully retrieved directly
            # FIX: Use str(target_node.nodeid)
            node_cache[str(target_node.nodeid)] = target_node
            node_display_names[str(target_node.nodeid)] = (await target_node.read_display_name()).Text
        except Exception as e:
            _logger.error(f"Could not find node '{node_identifier}' using client.get_node: {e}")
            print(f"Error: Node '{node_identifier}' not found or invalid.")
            return

    if not target_node:
        print(f"Error: Node '{node_identifier}' could not be resolved.")
        return

    try:
        # Get current value and its type to guide input conversion
        current_data_value = await target_node.read_data_value() # Await read_data_value
        current_variant = current_data_value.Value
        current_value = current_variant.Value
        target_variant_type = current_variant.VariantType

        # FIX: Use str(target_node.nodeid)
        print(f"Found node: '{(await target_node.read_display_name()).Text}' (NodeId: {str(target_node.nodeid)})")
        print(f"Current Value: {current_value} (Type: {target_variant_type.name})")

        value_to_write_str = input(f"Enter new value (e.g., {target_variant_type.name}): ").strip()
        if not value_to_write_str:
            print("No value entered. Aborting write operation.")
            return

        converted_value = _convert_input_to_opc_type(value_to_write_str, target_variant_type)
        if converted_value is None:
            print("Value conversion failed. Aborting write.")
            return

        # Create a Variant object with the correct type
        variant_to_write = ua.Variant(converted_value, target_variant_type)
        
        await target_node.write_value(variant_to_write) # Await write_value
        # FIX: Use str(target_node.nodeid)
        _logger.info(f"Successfully wrote '{converted_value}' to '{(await target_node.read_display_name()).Text}' ({str(target_node.nodeid)})")
        print(f"Successfully wrote '{converted_value}' to '{(await target_node.read_display_name()).Text}'.")

    except ua.UaError as e:
        _logger.error(f"OPC UA Error writing to node '{(await target_node.read_display_name()).Text}': {e}", exc_info=True)
        print(f"OPC UA Error: {e.local_description.Text} (Code: {e.code.name})")
    except Exception as e:
        _logger.error(f"An unexpected error occurred while writing to node '{(await target_node.read_display_name()).Text}': {e}", exc_info=True)
        print(f"An error occurred while writing: {e}")

# --- Main Client Application (Async) ---
async def main():
    client = Client(SERVER_URL)

    subscription = None
    try:
        _logger.info(f"Attempting to connect to {SERVER_URL}...")
        async with client: # Use async with for graceful connect/disconnect
            _logger.info("Connection successful!")

            handler = SubHandler()
            subscription = await client.create_subscription(500, handler)

            _logger.info("Starting recursive browsing from 'Objects' folder and subscribing to data variables/properties...")
            objects_node = client.get_objects_node() # This returns a Node object, not a coroutine
            await browse_and_subscribe_recursive(objects_node, subscription, client) # Await recursive browsing
            
            _logger.info("Subscription setup complete. Monitoring data changes...")
            _logger.info("Type 'w' to write a value, 'q' to quit, or press Enter to keep monitoring.")

            # Run a separate task to handle user input
            input_task = asyncio.create_task(read_user_input())

            while True:
                done, pending = await asyncio.wait([input_task], return_when=asyncio.FIRST_COMPLETED, timeout=1.0)
                
                # If input_task is done, process its result
                for task in done:
                    user_input = task.result().strip().lower() # Ensure input is stripped and lowercased
                    if user_input == 'q':
                        _logger.info("Quit command received. Exiting.")
                        return # Exit the main coroutine
                    elif user_input == 'w':
                        await write_value_to_node(client) # Await write operation
                        # Recreate the input task to continue monitoring
                        input_task = asyncio.create_task(read_user_input())
                    else:
                        print("Unknown command. Type 'w' to write, 'q' to quit.")
                        input_task = asyncio.create_task(read_user_input())

                if not done:
                    pass

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
                await subscription.delete() # Await subscription deletion
                _logger.info("Subscription deleted.")
            except Exception as e:
                _logger.warning(f"Error deleting subscription: {e}")
        _logger.info("Client application finished.")

async def read_user_input():
    """Reads a line from stdin in an async-compatible way."""
    return await asyncio.to_thread(sys.stdin.readline)

if __name__ == "__main__":
    asyncio.run(main())