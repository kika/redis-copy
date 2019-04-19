import * as redis from 'handy-redis'

const redis_url = process.argv[2]
if(!redis_url || redis_url.length === 0) {
    console.error(`Usage: ${process.argv[1]} redis-url`)
    process.exit(1);
}

const red = redis.createHandyClient(redis_url, {return_buffers: true})

const batch_size = 10000

list_keys(red)
    .finally(() => red.quit())
    .then(() => console.error("Done"))
    .catch((err) => console.error(err))

type ScanResult = [number, string[]]

async function list_keys(rediz: redis.IHandyRedis) {
    
    let cursor = 0
    let keyCount = 0
    const stime = Date.now()

    const totalKeys = await rediz.dbsize()
    console.error(`${totalKeys} keys to go`)

    console.log(`"Key", "TTL (-1 no ttl, -2 error)"`)
    do {
        const result: ScanResult = await rediz.scan(cursor, ['COUNT', batch_size])
        cursor = result[0]
        const keys = result[1]
        const proms: Promise<number>[] = []
        keys.forEach(async (key) => {
            keyCount++
            proms.push(rediz.pttl(key))
        })
        const ttls = await Promise.all(proms)
        console.error(`${keyCount} keys processed ${ttls.length} current batch`)
        if(keys.length > 0) {
            keys.forEach((key, index) => {
                console.log(`"${key}", "${ttls[index]}"`)
            })
        }
    } while(cursor !== 0)
    waitFinish()

    function waitFinish() {
        if(keyCount >= totalKeys) {
                console.error(`Total keys, ${keyCount}`)
                const ttime = Date.now() - stime
                console.error(`Avg speed, ${keyCount * 1000 / ttime}, keys/sec`)
        } else {
            setTimeout(waitFinish, 0)
        }
    }
}
