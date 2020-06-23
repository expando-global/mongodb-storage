// import config from '../config';
import { MongoClient, Db } from 'mongodb';

// TODO: Don't forget debug mode

let GLOBAL_CONNECTION: MongoClient;

export function db(): Promise<Db> {
    if (!GLOBAL_CONNECTION) {
        let timer: NodeJS.Timeout;

        return new Promise((resolve) => {
            console.warn('Requesting database access, not connected yet');
            timer = setInterval(() => {
                if (GLOBAL_CONNECTION) {
                    clearInterval(timer);
                    resolve(GLOBAL_CONNECTION.db());
                }
            }, 100);
        });
    }

    return Promise.resolve(GLOBAL_CONNECTION.db());
}

export async function connectDb(uri?: string) {
    if (GLOBAL_CONNECTION)
        throw new Error("There's already an existing database connection");

    const client = new MongoClient(uri || process.env.MONGODB_URI || '', {
        useUnifiedTopology: true,
        useNewUrlParser: true,
    });

    console.info('Connecting database...');
    await client.connect();
    console.info('Database connected');

    GLOBAL_CONNECTION = client;

    (await db()).on('close', () => {
        console.info('Database disconnected');
    });
}

export async function disconnectDb() {
    if (!GLOBAL_CONNECTION) return;
    console.info('Disconnecting database...');
    GLOBAL_CONNECTION.close();
}
