// 1- Obter uma listagem do númerototal de unidade vendidaspor produtopara um determinado período. 
db.Receipt.aggregate([{$match: {"Products.SellStartDate":{$gte:ISODate("2011-05-30T23:00:00.000+00:00")},"Products.SellEndDate":{$lte: ISODate("2012-05-29T00:00:00.000+00:00")}}},
{"$unwind":{path: "$Products"}}, {$group:{"_id":{"ProductID":"$Products.ID"}, "Quantity":{$sum:"$Products.Quantity"}}}, {$sort:{"Quantity": -1}}])

// 2- Obter uma listagem do númeroo total de unidade vendidaspor clientepara um determinado período.
db.Receipt.aggregate([{$match: {"OrderDate":{$gte:ISODate("2011-05-30T23:00:00.000+00:00"),$lte:ISODate("2011-06-30T23:00:00.000+00:00")}}},{"$unwind":{path: "$Products"}}, 
{$group:{"_id":{"CustomerKey":"$Customer.CustomerKey"}, "Quantity":{$sum:"$Products.Quantity"}}}, {$sort:{"Quantity": -1}}])

// 3- Obter uma listagemdas vendas em que não existe um cliente válido associado
db.Receipt.aggregate([{$match: {"Customer":{"$eq":{}}}},{$project:{"_id":0, "ReceiptID":1, "OrderDate":1, "Store":1, "StoreName":1}}])

// 4- Obter uma listagemdos produtos descontinuados
db.Receipt.aggregate([{$match: {"Products.SellEndDate":{"$ne":"NULL"}}}, {$project:{"_id":0,"Products.ID":1,"Products.Number":1,"Products.Name":1}}])

// 5- Obter uma listagem dos clientes que compram nenhum produto hámais de um mês
db.Receipt.aggregate([{$match: {"OrderDate":{"$lt" : new Date(ISODate().getTime() - 1000 * 3600 * 24 * 31)}}},{$project:{"_id":0, "Customer.CustomerKey":1}}])

// 6- Obter uma listagemdos produtos que não vendidos hámais de uma semana
db.Receipt.aggregate([{$match: {"OrderDate":{"$lt" : new Date(ISODate().getTime() - 1000 * 3600 * 24 * 7)}}},{"$unwind":{path: "$Products"}},
{$project:{"_id":0, "Products.ID":1, "Products.Name":1}}])



//Ex3, outra maneira de fazer
//db.Receipt.aggregate([{$lookup: {from:"CustomerDeatils", localField:"Customer.CustomerKey",foreignField:"CustomerKey",as:"CustomerKey"}},{$match: {"CustomerKey":{"$ne":"Customer.CustomerKey"}}},{"$unwind":{path: "$Customer"}},{$project:{"_id":0, "ReceiptID":1, "OrderDate":1, "Store":1, "StoreName":1}}])