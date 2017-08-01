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
    server: httpServer,

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

In some preliminary tests (that should not be taken for official benchmarks), `gun-mongo-key` can write ~10k key:values/sec, where each node had 10 key:value pairs. This equated to about ~2,500k nodes/sec. Read stats pending.

# Issues & Contributing

Issues welcome on [Github](https://github.com/sjones6/gun-mongo-key/issues).

Community contributions welcome. PRs accepted after code review.
