import * as redis from 'handy-redis'

const redis_url_from = process.argv[2]
const redis_url_to = process.argv[3]
if(!redis_url_from || redis_url_from.length === 0 || 
   !redis_url_to || redis_url_to.length === 0) {
  console.error(`Usage: ${process.argv[1]} redis-url-from redis-url-to`)
  process.exit(1);
}

const redis_from = redis.createHandyClient(redis_url_from, {return_buffers: true})
const redis_to = redis.createHandyClient(redis_url_to, {return_buffers: true})

const batch_size = 10000
const ignore_dupes = true

type ScanResult = [string, string[]]

copy_data(redis_from, redis_to)
  .then(() => console.log("Done"))
  .catch((err) => {
    console.error(err)
    process.exitCode = -1;
  })

async function copy_data(from: redis.IHandyRedis, to: redis.IHandyRedis) {
  
  let cursor: number = 0
  let keyCount: number = 0
  let readCount: number = 0
  let restoredCount: number = 0
  let restoreErrCount: number = 0
  let readErrCount: number = 0
  let skippedCount: number = 0

  const stime = Date.now()
  let message = ""

  const totalKeys = await from.dbsize()
  console.log(`Total keys: ${totalKeys}`)

  do {
    const result: ScanResult = await from.scan(cursor, ['COUNT', batch_size])
    cursor = parseInt(result[0]);
    const keys = result[1]
    keys.forEach(async (key) => {
      keyCount++

      try {
        const dump = await from.dump(key)
        const ttl = await from.pttl(key)
        readCount++
        // if the TTL is -1 then there's no TTL, which means 0
        // So in Redis universe -1 == 0 :-)
        const newttl = ttl === -1 ? 0 : ttl
        // Handle other negative values and skip restore
        if(newttl >= 0) {
          try {
            await to.restore(key, newttl, dump)
            restoredCount++
          }
          catch(err) {
            if(ignore_dupes && err.code === 'BUSYKEY') {
              restoredCount++
            } else {
              restoreErrCount++
              console.error(`RESTORE error: ${key}:${ttl} => ${err}`)
              process.exitCode = -1;
            }
          }
          message = `\x1b[${message.length}D${keyCount} keys`
          process.stdout.write(message)
        } else {
          // No TTL
          skippedCount++
        }
      }
      catch (err) {
        readErrCount++
        console.error(`read error: ${key} => ${err}`)
        process.exitCode = -1;
      }
    })
  } while(cursor !== 0)
  process.stdout.write('\n')
  return waitFinish()

  async function waitFinish() {
    if(keyCount >= totalKeys && readCount + readErrCount >= keyCount &&
       restoredCount + restoreErrCount + skippedCount >= readCount ) {
        console.log(`\nTotal keys: ${keyCount}`)
        console.log(`Restore errors: ${restoreErrCount}`)
        console.log(`Read errors: ${readErrCount}`)
        console.log(`Skipped keys: ${skippedCount}`)
        const ttime = Date.now() - stime
        console.log(`Avg speed: ${keyCount * 1000 / ttime} keys/sec`)
        await from.quit()
        await to.quit()
    } else {
      setTimeout(waitFinish, 0)
    }
  }
}
