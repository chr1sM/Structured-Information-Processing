

//-----------------------------------------------------------------------
//Criar a coleção Productprices guardada na variavel productprices
var productprices = db.ProductDetails.aggregate([
{ "$lookup": {from: 'ProductLPH',   localField: 'ProductID',   foreignField: 'ProductID',   as: 'Prices' }},
{ "$project":{    "ProductID": 1, "Name": 1, "ProductNumber": 1, "Color": 1, "Prices": "$Prices.Price", "SellStartDate": 1,"SellEndDate": 1}},
{$unwind:{ path: "$Prices"}}]).toArray()

//Gravar em Ficheiro
productprices.forEach(
function(doc){
db.ProductPrices.save(doc);
})

//-----------------------------------------------------------------------
//Colocar a SellEndDate de string para date nos campos diferenets de null
db.ProductPrices.find({"SellEndDate":{"$ne":"NULL"}}).forEach(function(doc) { 
    doc.SellEndDate= ISODate(doc.SellEndDate);
    db.ProductPrices.save(doc); 
})

//---------------------------------------------------------------------
//Criar a coleção SalesWCP guardada na variavel saleswcp. Limitada a 1000 documentos
var saleswcp =  db.SalesDetails.aggregate([
{$lookup: {from: 'CustomerDetails',   localField: 'Customer',   foreignField: 'CustomerKey',   as: 'Customer' }},
{$lookup: {from: 'ProductPrices',   localField: 'ProductID',   foreignField: 'ProductID',   as: 'Product' }},
{$lookup: {from: 'CurrencyDetails',localField: 'CurrencyRateID',foreignField: 'CurrencyRateID',as: 'Currency'}},
{$project: {"ReceiptID" : 1,"OrderDate" : 1,"Customer" : 1,"Currency": 1,"SubTotal" : 1,"TaxAmt" : 1,"Store" : 1,"StoreName" : 1,"ReceiptLineID" : 1,
"Quantity" : 1,"Product" : 1,"UnitPrice" : 1,"LineTotal" : 1}},
{$limit: 1000}]).toArray()

//Gravar em Ficheiro
saleswcp.forEach(
function(doc){
db.SalesWCP.save(doc);
})

//---------------------------------------------------------------------
//Criar a coleção Receipt guardada na variavel receipt
var receipt = db.SalesWCP.aggregate([
{$unwind:{path: "$Customer"}},
{$unwind:{path: "$Currency"}},
{$unwind:{path: "$Product"}},
{$project:{"ReceiptID": "$ReceiptID","OrderDate": "$OrderDate","Store": "$Store","StoreName": "$StoreName","Customer": {"CustomerKey": "$Customer.CustomerKey",
 "FirstName": "$Customer.FirstName","MiddleName": "$Customer.MiddleName","LastName": "$Customer.LastName","Gender":"$Customer.Gender","Email": "$Customer.EmailAddress",
 "AddressLine1": "$Customer.AddressLine1","AddressLine2": "$Customer.AddressLine2","Phone": "$Customer.Phone","DateFirstPurchase": "$Customer.DateFirstPurchase"},
 "Products": {"ID":"$Product.ProductID","Name": "$Product.Name","Number": "$Product.Name","Color": "$Product.Color","Quantity": "$Quantity","Price": "$Product.Prices",
 "UnitPrice": "$UnitPrice","LineTotal": "$LineTotal","SellStartDate":"$Product.SellStartDate","SellEndDate":"$Product.SellEndDate"},"Currency": "$Currency",
  "TaxAmt": "$TaxAmt","SubTotal": "$SubTotal"}},
{$group:{"_id":{"ReceiptID":"$ReceiptID","OrderDate":"$OrderDate","Store":"$Store","StoreName":"$StoreName","Customer":"$Customer","Currency":"$Currency",
"TaxAmt":"$TaxAmt","SubTotal":"$SubTotal"},"Products":{"$push":"$Products"}}},
{$project:{ "_id":0,"ReceiptID": "$_id.ReceiptID","OrderDate": "$_id.OrderDate","Store": "$_id.Store","StoreName": "$_id.StoreName","Customer":"$_id.Customer","Products": "$Products",
"Currency": "$_id.Currency","TaxAmt": "$_id.TaxAmt","SubTotal": "$_id.SubTotal"}}]).toArray()

//Gravar em Ficheiro
receipt.forEach(
function(doc){
db.Receipt.save(doc);
})




