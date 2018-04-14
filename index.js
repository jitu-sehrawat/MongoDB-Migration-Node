const express = require('express')
const logger = require('morgan')
const errorhandler = require('errorhandler')
const mongodb = require('mongodb')
const bodyParser = require('body-parser')
const url = 'mongodb://localhost:27017/edx-course-db'
const customers = require('./m3-customer-data.json')
const customerAddresses = require('./m3-customer-address-data.json')
const async = require('async')


let tasks = []
const limit = parseInt(process.argv[2]) || 1000
mongodb.MongoClient.connect(url, (error, db) => {
  if (error) return process.exit(1)
  customers.forEach((customer, index, list) => {
    customers[index] = Object.assign(customer, customerAddresses[index])

    if (index % limit == 0) {
      const start = index
      const end = (start+limit > customers.length) ? customers.length-1 : start+limit
      tasks.push((done) => {
        console.log(`Inserting customer data from ${start}-${end}`)
        db.collection('customers').insert(customers.slice(start, end), (error, results) => {
          done(error, results)
        })
      })
    }
  })
  console.log(`Launching : ${(1000)/parseInt(process.argv[2])}`)
  async.parallel(tasks, (error, results) => {
    if (error) console.error(error)
    else{
      console.log('Success');
    }
    db.close()
  })
})
