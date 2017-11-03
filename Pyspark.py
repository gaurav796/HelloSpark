#Get revenue for given order_id, by adding order_item_subtotal from order_items
#Read data from local file system /data/retail_db/order_items/part-00000
orderItems = open("/data/retail_db/order_items/part-00000").read().splitline()

orderItemsFiltered = filter(lambda s: int(s.split(",")[1]) == 5, orderItems)
orderItemsMap = map(lambda s: float(s.split(",")[4]), orderItemsFiltered)
orderRevenue = reduce(lambda totalRevenue, itemRevenue: totalRevenue + itemRevenue, orderItemsMap)
orderMinSubtotal = reduce(lambda minRevenue, itemRevenue: minRevenue if(minRevenue < itemRevenue) else itemRevenue, orderItemsMap)

orders = sc.textFile("/public/retail_db/orders")
for i in orders.take(10): print(i)

orderItems = sc.textFile("/public/retail_db/order_items")
for i in orderItems.take(10): print(i)

for i in orders.\
map(lambda s: s.split(",")[3]).\
distinct().collect(): 
  print(i)

ordersFiltered = orders.\
filter(lambda s: s.split(",")[3] == "COMPLETE" or s.split(",")[3] == "CLOSED")

ordersFilteredMap = ordersFiltered.\
map(lambda o: (int(o.split(",")[0]), o.split(",")[1]))
orderItemsMap = orderItems.\
map(lambda o: (int(o.split(",")[1]), float(o.split(",")[4])))
ordersJoin = ordersFilteredMap.join(orderItemsMap)
ordersJoinMap = ordersJoin.map(lambda t: t[1])
dailyRevenue = ordersJoinMap.\
reduceByKey(lambda totalRevenue, orderItemRevenue: totalRevenue + orderItemRevenue)

dailyRevenueSorted = dailyRevenue.sortByKey()

for i in dailyRevenueSorted.collect(): print(i)
