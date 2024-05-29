import asyncio
import nats
import json
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError

async def main():
    nc = await nats.connect("nats://public:thenewalbiondata@nats.albion-online-data.com:4222")

    try:
        # Simple publisher and async subscriber via coroutine.
        # sub = await nc.subscribe("marketorders.ingest", cb=message_handler)
        sub = await nc.subscribe("marketorders.deduped.bulk", cb=message_handler)

        # Keep the connection alive indefinitely
        while True:
            await asyncio.sleep(1)  # keep the event loop running
    finally:
        await nc.close()

async def message_handler(msg):
    subject = msg.subject
    reply = msg.reply
    data = json.loads(msg.data.decode())

    # Set the specific ItemTypeId and LocationId you're looking for
    specific_item_type_id = 'T3_JOURNAL_MAGE_FULL'
    specific_location_id = 3005

    # Check if any of the orders match the specific ItemTypeId and LocationId
    # for order in data['Orders']:
    for order in data:
        if order['ItemTypeId'] == specific_item_type_id and order['LocationId'] == specific_location_id:
            print("Found an order with ItemTypeId '{0}' and LocationId '{1}'".format(specific_item_type_id, specific_location_id))
            print(order)
    
if __name__ == '__main__':
    asyncio.run(main())