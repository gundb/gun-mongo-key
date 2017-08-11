const {Flint} = require('gun-flint');
const GunMongoKey = require('./gun-mongo-key');
module.exports = Flint.register(GunMongoKey);