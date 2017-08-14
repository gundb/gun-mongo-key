# Gun-Mongo

A key:value MongoDB adapter for GunJS.

# Some Considerations

`gun-mongo-key` stores the each graph node's key:value (along with some metadata) in a separate document. This provides some major advantages in two areas: streaming large nodes (so that memory isn't overwhelmed) and making updates faster. The disadvantage is that creating new nodes (especially) nodes with lots of properties will be less efficient.

If the following are true, then `gun-mongo-key` could be right for your application:

1. You anticipate large nodes (e.g., a `users` node with millions of children will be streamed to Gun rather than transferred in the entirety).
2. Nodes or node properties are updated frequently. Updates (_especially_ of large nodes) of individual properties will be faster and more efficient than full-node storage.
3. Updating existing nodes is more common/important than creating new nodes.

Contrast this with [`gun-mongo`](https://github.com/sjones6/gun-mongo) which has an advantages for creating nodes. This advantage dwindles at the point where a node becomes so large that it threatens to overwhelm memory.

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
        query: ''
    }
});
```

# Performance

Tests run on a 2012 Macbook Pro, 2.5 GHz Intel Core i5, 16 GB RAM.

**Starting with empty collection**:
Write 10000 nodes: : 11579ms; 11.579s; 1.1579 ms/node.
Read 10000 nodes: : 18681ms; 18.681s; 1.8681 ms/node.
Update 10000 nodes: : 12798ms; 12.798s; 1.2798 ms/node.

**1.2 million existing documents in the collection**:
Write 10,000 nodes: : 11,858ms; 11.858s; 1.1858 ms/node.
Read 10,000 nodes: : 26,289ms; 26.289s; 2.6289 ms/node.
Update 10,000 nodes: : 12,308ms; 12.308s; 1.2308 ms/node.

# Issues & Contributing

Issues welcome on [Github](https://github.com/sjones6/gun-mongo-key/issues).

Community contributions welcome. PRs accepted after code review.
