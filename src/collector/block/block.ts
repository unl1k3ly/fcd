import * as fs from 'fs'
import * as path from 'path'
import * as Bluebird from 'bluebird'
import { EntityManager, In } from 'typeorm'
import { compact, chunk } from 'lodash'

import { BlockEntity, TxEntity, AccountTxEntity } from 'orm'

import * as lcd from 'lib/lcd'
import { collectorLogger as logger } from 'lib/logger'
import { generateAccountTxs } from './accountTx'

const UNWANTED_HASH_FILE = path.resolve(__dirname, 'unwanted_hashes.json')

function getUnwantedHashes(): string[] {
  if (!fs.existsSync(UNWANTED_HASH_FILE)) {
    fs.writeFileSync(UNWANTED_HASH_FILE, JSON.stringify({ unwantedHashes: [] }, null, 2))
  }
  const data = fs.readFileSync(UNWANTED_HASH_FILE, 'utf-8')
  return JSON.parse(data).unwantedHashes || []
}

function addUnwantedHash(hash: string): void {
  const unwantedHashes = getUnwantedHashes()
  if (!unwantedHashes.includes(hash)) {
    unwantedHashes.push(hash)
    fs.writeFileSync(UNWANTED_HASH_FILE, JSON.stringify({ unwantedHashes }, null, 2))
    logger.info(`Added problematic hash to unwanted list: ${hash}`)
  }
}

export async function generateTxEntity(tx: Transaction.LcdTransaction, block: BlockEntity): Promise<TxEntity> {
  const txEntity = new TxEntity()
  txEntity.chainId = block.chainId
  txEntity.hash = tx.txhash.toUpperCase()
  txEntity.data = await sanitizeTx(tx)
  txEntity.timestamp = new Date(tx.timestamp)
  txEntity.block = block
  return txEntity
}

// Recursively iterating through the keys of the tx object to find and sanitize unicode characters
async function sanitizeTx(tx: Transaction.LcdTransaction): Promise<Transaction.LcdTransaction> {
  function hasUnicodeOrControl(s: string): boolean {
    // eslint-disable-next-line no-control-regex
    return /[^\u0020-\u007f]/.test(s)
  }

  const iterateTx = (obj: any) => {
    Object.keys(obj).forEach((key) => {
      if (typeof obj[key] === 'object' && obj[key] !== null) {
        iterateTx(obj[key])
      } else {
        if (hasUnicodeOrControl(obj[key])) {
          const b = Buffer.from(obj[key])
          obj[key] = b.toString('base64')
        }
      }
    })
  }
  iterateTx(tx)
  return tx
}

async function generateTxEntities(txHashes: string[], block: BlockEntity): Promise<TxEntity[]> {
  const txHashesUnique = [...new Set(txHashes)]
  const unwantedHashList = getUnwantedHashes()

  const filteredTxHashes = txHashesUnique.filter((txHash) => !unwantedHashList.includes(txHash))

  return Bluebird.map(filteredTxHashes, async (txHash) => {
    try {
      const tx = await lcd.getTx(txHash)
      return generateTxEntity(tx, block)
    } catch (err) {
      logger.error(`Error fetching txHash ${txHash}: ${err.message}`)
      addUnwantedHash(txHash) // Add problematic hash to the unwanted list
      return null // Skip this transaction
    }
  }).filter((txEntity) => txEntity !== null) // Remove null values
}

export async function collectTxs(mgr: EntityManager, txHashes: string[], block: BlockEntity): Promise<TxEntity[]> {
  const txEntities = await generateTxEntities(txHashes, block)

  const existingTxs = await mgr.find(TxEntity, { where: { hash: In(txEntities.map((t) => t.hash)) } })

  existingTxs.forEach((e) => {
    if (!e.data.code) {
      const idx = txEntities.findIndex((t) => t.hash === e.hash)

      if (idx < 0) {
        throw new Error('impossible')
      }

      logger.info(`collectTxs: existing successful tx found: ${e.hash}`)
      txEntities.splice(idx, 1)
    }
  })

  // Save TxEntity
  const qb = mgr
    .createQueryBuilder()
    .insert()
    .into(TxEntity)
    .values(txEntities)
    .orUpdate(['timestamp', 'data', 'block_id'], ['chain_id', 'hash'])

  await qb.execute()

  // Generate and save AccountTxEntities
  const accountTxs: AccountTxEntity[] = compact(txEntities)
    .map((txEntity) => generateAccountTxs(txEntity))
    .flat()

  await Bluebird.mapSeries(chunk(accountTxs, 5000), (chunk) => mgr.save(chunk))

  logger.info(`collectTxs: ${txEntities.length}, accountTxs: ${accountTxs.length}`)
  return txEntities
}
