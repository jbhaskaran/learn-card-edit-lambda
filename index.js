const path = require('path')
process.env.APP_PATH = path.resolve(__dirname, 'node_modules/@glueit/learn-back/')

const storeConfig = require('@glueit/back/src/storeConfig')
const storeInit = require('@glueit/back/src/storeInit')
const store = require('@glueit/back/src/store')

let storeAdapter = null

const entryMap = [
  { pk: '@tag', sk: '%tagName%' },
  { pk: '@tag-%tagName%', sk: '@updatedTime-%updatedTime%' },
  { pk: '@tag-%tagName%-@level-%levelName%', sk: '@updatedTime-%updatedTime%' },
  { pk: '@tag-%tagName%-@type-%component%', sk: '@updatedTime-%updatedTime%' },
  {
    pk: '@tag-%tagName%-@type-%component%-@level-%levelName%',
    sk: '@updatedTime-%updatedTime%'
  },
  { pk: '@user-%userId%-@tag-%tagName%', sk: '@updatedTime-%updatedTime%' },
  {
    pk: '@user-%userId%-@tag-%tagName%-@level-%levelName%',
    sk: '@updatedTime-%updatedTime%'
  },
  {
    pk: '@user-%userId%-@tag-%tagName%-@type-%component%',
    sk: '@updatedTime-%updatedTime%'
  },
  {
    pk: '@user-%userId%-@tag-%tagName%-@type-%component%-@level-%levelName%',
    sk: '@updatedTime-%updatedTime%'
  },
  { pk: '@user-%userId%', sk: '@updatedTime-%updatedTime%' },
  { pk: '@type-%component%', sk: '@updatedTime-%updatedTime%' },
  { pk: '@latest', sk: '@updatedTime-%updatedTime%' }
]

const replaceTokens = ({ string, tokens }) => {
  let replacementStr = string
  for (let name in tokens) {
    replacementStr = replacementStr.replace(
      new RegExp(`%${name}%`, 'g'),
      tokens[name]
    )
  }
  return replacementStr
}

exports.handler = async event => {
  const storeName = 'primary'
  const dynamoStoreName = 'cardsBy'
  let isLocal = false
  if (!storeAdapter) {
    const config = await storeConfig(storeName)
    storeAdapter = storeInit(config)
  }

  let messages = []
  if (event.Records) {
    messages = event.Records
  } else {
    isLocal = true
    messages = await store.get({ storeName: 'cardEditQueue', query: {} })
  }
  if (Array.isArray(messages)) {
    for (let i in messages) {
      const message = messages[i]
      const body = JSON.parse(message.body || message.Body)
      const name = body.objectName
      const id = body.id
      const updatedTime = body.updatedTime
      const deleteData = body.deleteData
      const card = await store.get({
        storeName,
        name,
        id,
        idName: 'id',
        query: {}
      })

      if (deleteData !== false) {
        const deleteObject = {
          sortKey: null
        }

        for (let entry of entryMap) {
          for (let tagName of deleteData.tags) {
            const tokens = {
              tagName: tagName.toLowerCase(),
              levelName: deleteData.level,
              updatedTime: deleteData.prevUpdatedTime,
              component: deleteData.component,
              userId: deleteData.userId
            }

            deleteObject.sortKey = replaceTokens({ string: entry.sk, tokens })
            await store.delete({
              storeName: dynamoStoreName,
              name,
              id: replaceTokens({ string: entry.pk, tokens }),
              idName: 'partitionKey',
              object: deleteObject
            })
          }
        }
      }

      const tags = card.tags
      if (card.meta.updatedTime === updatedTime) {
        const cardsByObject = {
          sortKey: null,
          id,
          userId: card.userId,
          cardName: card.name,
          tags: JSON.stringify(card.tags),
          component: card.component,
          cardLevel: card.level
        }

        for (let entry of entryMap) {
          for (let tagName of tags) {
            const tokens = {
              tagName: tagName.toLowerCase(),
              levelName: card.level,
              updatedTime: card.meta.updatedTime,
              component: card.component,
              userId: card.userId
            }
            cardsByObject.sortKey = replaceTokens({ string: entry.sk, tokens })

            await store.update({
              storeName: dynamoStoreName,
              name,
              id: replaceTokens({ string: entry.pk, tokens }),
              idName: 'partitionKey',
              object: cardsByObject
            })
          }
        }
      }
      if (isLocal) {
        const object = {
          receiptHandle: message.ReceiptHandle
        }
        await store.delete({ storeName: 'cardEditQueue', object })
      }
    }
  }
}