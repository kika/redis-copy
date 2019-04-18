import * as redis from 'handy-redis'

const redis_url = process.argv[2]
const pattern = process.argv[3]
if(!redis_url || redis_url.length === 0 || !pattern || pattern.length === 0) {
    console.error(`Usage: ${process.argv[1]} redis-url pattern-to-delete`)
    process.exit(1);
}

const red = redis.createHandyClient(redis_url, {return_buffers: true})

const batch_size = 10000
const dry_run = false

list_keys(red)
    .finally(() => red.quit())
    .then(() => console.error("Done"))
    .catch((err) => console.error(err))

async function list_keys(rediz: redis.IHandyRedis) {
    
    let cursor = 0
    let keyCount = 0
    let delCount = 0
    let message = ""
    const stime = Date.now()

    do {
        const result = await rediz.scan(
            cursor,
            ['MATCH', pattern],
            ['COUNT', batch_size]
        )
        cursor = parseInt(result[0])
        const keys = result[1] as [string]
        if(keys.length > 0) {
            keyCount += keys.length
            if(dry_run) {
                delCount += keys.length
            } else {
                delCount += await rediz.del.apply(null, keys)
            }
            message = `\x1b[${message.length}D${delCount} keys deleted`
            process.stdout.write(message)
        }
    } while(cursor !== 0)
    process.stdout.write('\n')
    waitFinish()

    function waitFinish() {
        if(delCount >= keyCount) {
                console.error(`Total keys deleted, ${delCount}`)
                const ttime = Date.now() - stime
                console.error(`Avg speed, ${delCount * 1000 / ttime}, keys/sec`)
        } else {
            setTimeout(waitFinish, 0)
        }
    }
}
