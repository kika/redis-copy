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
const ignore_dupes = false

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
    const stime = Date.now()

//    const totalKeys = await from.dbsize()

    do {
        const result = await from.scan(cursor, ['COUNT', batch_size])
        cursor = parseInt(result[0])
        const keys = result[1] as [string]
        keys.forEach((key) => {
            keyCount++
            from.dump(key).then((dump) => {
                dumpCount++
                from.pttl(key).then((ttl) => {
                    ttlCount++
                    const newttl = ttl === -1 ? 0 : ttl
                    to.restore(key, newttl, dump).then((_) => {
                        restoredCount++
                        willFinish()
                    }).catch((err) => {
                        if(ignore_dupes && err.code === 'BUSYKEY') {
                            restoredCount++
                        } else {
                            restoreErrCount++
                            console.error(`RESTORE error: ${key}:${ttl} => ${err}`)
                        }
                        willFinish()
                    })
                    willFinish()
                }).catch((err) => {
                    ttlErrCount++
                    console.error(`PTTL error: ${key} => ${err}`)
                    willFinish()
                })
            }).catch((err) => {
                dumpErrCount++
                console.error(`DUMP error: ${key} => ${err}`)
                willFinish()
            })
        })
    } while(cursor !== 0)

    function willFinish() {
        if(dumpCount + dumpErrCount === keyCount &&
           restoredCount + restoreErrCount === ttlCount &&
           ttlCount + ttlErrCount === dumpCount) {
                console.log(`Total keys: ${keyCount}`)
                console.log(`Dump errors: ${dumpErrCount}`)
                console.log(`Restore errors: ${restoreErrCount}`)
                console.log(`Ttl errors: ${ttlErrCount}`)
                const ttime = Date.now() - stime
                console.log(`Avg speed: ${keyCount * 1000 / ttime} keys/sec`)
                process.exit(
                    dumpErrCount === 0 &&
                    restoreErrCount === 0 &&
                    ttlErrCount === 0 ? 0 : -1
                )
           }
    }
}
