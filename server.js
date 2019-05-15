const mongodb = require('mongodb')
const fs = require('fs')
const async = require('async')

const pathBackupFile = 'data/m3-customer-address-data.json'
const pathMissingFile = 'data/m3-customer-data.json'
const mongoserver = 'mongodb://localhost:27017'
const dbName = 'edx-async'
const cCustomers = 'edx-customers'

const tasks = [];
var addresses, customers;

const limit = parseInt(process.argv[2], 10) || 1000

addresses = JSON.parse(fs.readFileSync(pathBackupFile))
customers = JSON.parse(fs.readFileSync(pathMissingFile))

const totalRecords = customers.length

mongodb.MongoClient.connect(mongoserver, { useNewUrlParser: true } , (err, client) =>  {
    if(err) return process.exit(1)
    console.log('Connected successfuly')
    let db = client.db(dbName)
    const startTime = Date.now()

    for (let index = 0; index < totalRecords; index++) {
        customers[index] = Object.assign(customers[index], addresses[index])
        
        if(index % limit == 0){
            const start = index;
            const end = (start + limit > customers.length) ? customers.length - 1 : start + limit
            tasks.push((callback)=> {
                db.collection(cCustomers)
                .insertMany(customers.slice(start, end), (err, result) => {
                    if(err) console.error(err)
                    callback(null, index)
                })
            })
        }
    }
    async.parallel(tasks, (err, results) => {
        if(err)  console.error(err)
        const endTime = Date.now()
        console.log(`Execution time: ${endTime - startTime}`)
        client.close();
    })
})

