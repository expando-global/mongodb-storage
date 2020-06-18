import { MongoMemoryServer } from 'mongodb-memory-server';
import { TestInterface } from 'ava';

import { connectDb, disconnectDb, db } from '../lib/db-connector';

const mongod = new MongoMemoryServer()
// const mongod = new MongoMemoryServer({
//     binary: {
//         // downloadDir: '/path/to/mongodb/binaries',
//         // platform: 'linux',
//         // arch: 'x64',
//         version: '4',
//         // debug: '1',
//         // downloadMirror: 'url',
//         // disablePostinstall: '1',
//         // systemBinary: '/usr/local/bin/mongod',
//         // md5Check: '1',
//     },
// });

export function prepareDb(test: TestInterface) {
    test.before(async () => {
        await connectDb(await mongod.getUri());
    });

    test.afterEach(async () => {
        const colls = await (await db()).collections();
        colls.forEach((coll) => coll.deleteMany({}));
    });

    test.after.always(async () => {
        disconnectDb();
        mongod.stop();
    });
}
