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

type ScanResult = [number, string[]]

copy_data(redis_from, redis_to)
    .then(() => console.log("Done"))
    .catch((err) => console.error(err))

async function copy_data(from: redis.IHandyRedis, to: redis.IHandyRedis) {
    
    let cursor = 0
    let keyCount = 0
    let dumpCount = 0
    let dumpErrCount = 0
    let restoredCount = 0
    let restoreErrCount = 0
    let ttlCount = 0
    let ttlErrCount = 0
    let skippedCount = 0

    const stime = Date.now()
    let message = ""

    const totalKeys = await from.dbsize()
    console.log(`Total keys: ${totalKeys}`)

    do {
        const result: ScanResult = await from.scan(cursor, ['COUNT', batch_size])
        cursor = result[0]
        const keys = result[1]
        keys.forEach((key) => {
            keyCount++
            from.dump(key).then((dump) => {
                dumpCount++
                from.pttl(key).then((ttl) => {
                    ttlCount++
                    // if the TTL is -1 then there's no TTL, which means 0
                    // So in Redis universe -1 == 0 :-)
                    const newttl = ttl === -1 ? 0 : ttl
                    // Handle other negative values and skip restore
                    if(newttl >= 0) {
                        to.restore(key, newttl, dump)
                        .then((_) => {
                            restoredCount++
                        }).catch((err) => {
                            if(ignore_dupes && err.code === 'BUSYKEY') {
                                restoredCount++
                            } else {
                                restoreErrCount++
                                console.error(`RESTORE error: ${key}:${ttl} => ${err}`)
                            }
                        })
                        .then(() => {
                            message = `\x1b[${message.length}D${keyCount} keys`
                            process.stdout.write(message)
                        })
                        .catch() // Dummy catch to satisfy TS compiler
                    } else {
                        skippedCount++
                    }
                }).catch((err) => {
                    ttlErrCount++
                    console.error(`PTTL error: ${key} => ${err}`)
                })
            }).catch((err) => {
                dumpErrCount++
                console.error(`DUMP error: ${key} => ${err}`)
            })
        })
    } while(cursor !== 0)
    process.stdout.write('\n')
    return waitFinish()

    async function waitFinish() {
        if(keyCount >= totalKeys && dumpCount + dumpErrCount >= keyCount &&
           restoredCount + restoreErrCount >= ttlCount &&
           ttlCount + ttlErrCount + skippedCount >= dumpCount) {
                console.log(`Total keys: ${keyCount}`)
                console.log(`Dump errors: ${dumpErrCount}`)
                console.log(`Restore errors: ${restoreErrCount}`)
                console.log(`Ttl errors: ${ttlErrCount}`)
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
