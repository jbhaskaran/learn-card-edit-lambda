const assert = require('assert')
const AWS = require('aws-sdk')
// AWS.config.update({ region: 'REGION' })
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' })

module.exports = {
  get: async ({ queueName }) => {
    assert(process.env.GLUEIT_QUEUE_ENDPOINT, 'Queue URL missing')

    const QueueUrl = `${process.env.GLUEIT_QUEUE_ENDPOINT}${queueName}`
    const params = {
      AttributeNames: ['SentTimestamp'],
      MaxNumberOfMessages: 10,
      MessageAttributeNames: ['All'],
      QueueUrl,
      VisibilityTimeout: 20,
      WaitTimeSeconds: 0
    }
    const messages = new Promise((res, rej) => {
      sqs.receiveMessage(params, (err, data) => {
        if (err) {
          rej(err)
        } else if (data.Messages) {
          res(data.Messages)
        }
      })
    })
    return messages
  },
  add: async ({ queueName, object }) => {
    assert(process.env.GLUEIT_QUEUE_ENDPOINT, 'Queue URL missing')

    const QueueUrl = `${process.env.GLUEIT_QUEUE_ENDPOINT}${queueName}`
    const params = {
      DelaySeconds: 10,
      MessageBody: JSON.stringify(object),
      QueueUrl
    }
    const result = new Promise((res, rej) => {
      sqs.sendMessage(params, (err, data) => {
        if (err) {
          rej(err)
        } else {
          res(data)
        }
      })
    })
    return result
  },
  delete: async ({ queueName, ReceiptHandle }) => {
    assert(process.env.GLUEIT_QUEUE_ENDPOINT, 'Queue URL missing')

    const QueueUrl = `${process.env.GLUEIT_QUEUE_ENDPOINT}${queueName}`
    const deleteParams = {
      QueueUrl,
      ReceiptHandle
    }
    const result = await new Promise((res, rej) => {
      sqs.deleteMessage(deleteParams, (err, data) => {
        if (err) {
          rej(err)
        } else {
          res(data)
        }
      })
    })
    return result
  }
}
