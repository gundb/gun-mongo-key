# Gun-Mongo-Key

A key:value MongoDB adapter for GunJS.

# Some Considerations

`gun-mongo-key` stores the each graph node's key:value (along with some metadata) in a separate document. This provides some major advantages in two areas:

* streaming large nodes (so that memory isn't overwhelmed)
* updating single-property's at constant speed (irrespective of node size)

The disadvantage is that creating new nodes (especially) nodes or entirely rewriting with lots of properties will be less efficient.

If the following are true, then `gun-mongo-key` could be right for your application:

1. You anticipate large nodes (e.g., a `users` node with millions of children will be streamed to Gun rather than transferred in the entirety).
2. Node properties are updated frequently. Updates (_especially_ of large nodes) of individual properties will be faster and more efficient than full-node storage.
3. Updating existing nodes is more common/important than creating new nodes.

Contrast this with [`gun-mongo`](https://github.com/sjones6/gun-mongo) which has an advantages for creating and updating of full nodes. This advantage dwindles at the point where a node becomes so large that it threatens to overwhelm memory.

# Installation

`yarn add gun-mongo-key` or `npm install gun-mongo-key`.

```javascript

const Gun = require('gun');

// Must be added after Gun but before instantiating Gun
require('gun-mongo-key');

// Instantiate Gun
const gun = new Gun({
    file: false,
    web: httpServer,

    // The following are defaults. You can supply `true` to use all defaults
    mongo: {
        host: 'localhost',
        port: '27017',
        database: 'gun',
        collection: 'gun_mongo_key',
        query: '',
        opt: {
            poolSize: 25 // how large is the connection pool
            // include any other options when initializing:
            // See: http://mongodb.github.io/node-mongodb-native/2.2/reference/connecting/connection-settings/
        },
        chunkSize: 250 // see below
    }
});
```

# Performance

Tests run on a 2012 Macbook Pro, 2.5 GHz Intel Core i5, 16 GB RAM.

**Small Nodes: 10 Properties Each**
* Write 10000 nodes: 25558ms; 25.558s; **2.556 ms/node**
* Read 10000 nodes: 10365ms; 10.365s; **1.036 ms/node**
* Full Update 10000 nodes: 22895ms; 22.895s; **2.289 ms/node**
* Update single field on 10000 nodes: 9299ms; 9.299s; **0.930 ms/node**

**Medium Nodes: 1000 Properties Each**
* Write 1000 nodes: 133160ms; 133.16s; **133.027 ms/node**
* Read 1000 nodes: 21993ms; 21.993s; **21.971 ms/node**
* Full 1000 nodes: 143452ms; 143.452s; **143.309 ms/node**
* Update single field on 1000 nodes: : 1298ms; 1.298s; **1.297 ms/node**

## Performance Tuning

There are a few things that you can do to tune this adapter to your particular application's needs (without sharding your DB, or other measures).

### Chunking Results

For optimal performance, GunMongoKey will chunk key:value results into groups of 250 to stream back to Gun. The default is set fairly high at 250. If you anticipate that nodes might contain large values (e.g., 250 key:value pairs might overwhelm the memory), then you may want to set this lower. If all of the values are small (even if the node itself has many keys), consider setting this higher.

### Pool Size

Allowing a larger pool of connections can provide overall application speed by increasing the number of concurrent operations; however, too many connections can also have adverse effects. 

# Issues & Contributing

Issues welcome on [Github](https://github.com/sjones6/gun-mongo-key/issues).

Community contributions welcome. PRs accepted after code review.
