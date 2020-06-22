import {
    FilterQuery,
    IndexSpecification,
    ObjectId,
    UpdateQuery,
    PushOperator,
} from 'mongodb';
import Joi, { JoiObject } from 'typesafe-joi';
import _ from 'lodash';

import { db } from './db-connector';
import { convertObjectWithPattern } from 'expando-convert-pattern';
import { Money, MoneySchema } from 'expando-money';
import { IChangelog } from '../schemas/changelog';
import { IRequestContext } from '../schemas/request-context';

import { diff } from 'deep-diff';

interface Document {
    [key: string]: any;
    changelogs?: any;
}

function serialize<T>(document: T | Partial<T>): T {
    // TODO: Do a better serialization of Date
    return JSON.parse(JSON.stringify(document), (key, value) => {
        return (key.endsWith('Id') || key === '_id') && ObjectId.isValid(value)
            ? new ObjectId(value)
            : value;
    });
}

export function createChangelog<T>(
    rc: IRequestContext,
    originalOrder: T,
    updates: Partial<T>,
): IChangelog {
    const updatesOnOriginal = Object.assign(
        _.cloneDeep(originalOrder),
        updates,
    );
    const difference = diff(originalOrder, updatesOnOriginal);

    return {
        ip: rc.ip,
        token: rc.token,
        endpoint: rc.endpoint,
        timestamp: new Date(),
        changes: JSON.parse(JSON.stringify(difference || [])),
    };
}

export function makeStorage<T extends Document>(
    documentName: string,
    collectionName: string,
    documentSchema: JoiObject,
    indexes: IndexSpecification[],
) {
    const collection = async () => {
        return (await db()).collection(collectionName);
    };

    const ensureCollection = async () => {
        const collectionNames = (await (await db()).collections()).map(
            (coll) => coll.collectionName,
        );

        if (!collectionNames.includes(collectionName)) {
            console.warn(`Creating new collection '${collectionName}'`);
            return (await db()).createCollection(collectionName);
        }

        return collection();
    };

    const ensureIndexes = async (indexes: IndexSpecification[]) => {
        const ixSpecificationIdentity = (i: IndexSpecification) =>
            JSON.stringify(i.key);

        const existingIndexes = await (await collection())
            .listIndexes()
            .toArray();

        const newIndexes = _.differenceBy(
            indexes,
            existingIndexes,
            ixSpecificationIdentity,
        );

        const removedIndexes = _.differenceBy(
            existingIndexes,
            indexes,
            ixSpecificationIdentity,
        ).filter(
            (index) =>
                // ignore default _id index
                ixSpecificationIdentity(index) !== JSON.stringify({ _id: 1 }),
        );

        if (newIndexes.length) {
            console.info(
                `Creating ${newIndexes.length} new indexes on '${collectionName}'`,
            );
            await (await collection()).createIndexes(
                newIndexes.map((i) => ({ ...i, background: true })),
            );
        }

        if (removedIndexes.length) {
            console.info(
                `Removing ${removedIndexes.length} indexes on '${collectionName}'`,
            );
            for (const index of removedIndexes) {
                await (await collection()).dropIndex(index.name);
            }
        }
    };

    /**
     * This validates the document against provided schema, except field
     * `changelogs`.
     *
     * `changelogs` is managed by storage layer and should not be part of
     * user input.
     */
    const validate = (document: T) => {
        const { error, value } = Joi.validate(
            document,
            // @ts-ignore
            documentSchema.append({
                changelogs: Joi.optional(),
            }),
            {
                stripUnknown: true,
            },
        );

        if (error)
            throw new Error(
                `Error validating '${collectionName}' document: ${error.message}`,
            );

        return value;
    };

    /**
     * Maps database documents to format that is valid for the application.
     * It is used mostly for conversion of object abstractions, etc
     */
    const mapDocumentFromDb = (doc: any): T => {
        const mappedDoc = convertObjectWithPattern<T>(
            doc,
            Object.keys(MoneySchema.describe().children),
            Money.fromMoney,
        );

        return mappedDoc;
    };

    ensureCollection().then(() => ensureIndexes(indexes));

    return {
        insertOne: async function (rc: IRequestContext, document: any) {
            validate(document);

            const result = await (await collection()).insertOne(
                Object.assign(serialize(document), {
                    // add a changelog with empty changes to track
                    // where the document came from in the first place
                    changelogs: [createChangelog(rc, {}, {})],
                }),
            );

            return mapDocumentFromDb(_.omit(result.ops[0], '_id'));
        },

        findOne: async function (filter: FilterQuery<any> = {}) {
            const foundDocument = await (await collection()).findOne(filter, {
                projection: { _id: 0 },
            });
            if (!foundDocument)
                throw new Error(
                    documentName +
                        (filter._id ? ` #${filter._id}` : '') +
                        " doesn't exist",
                );
            return mapDocumentFromDb(foundDocument);
        },

        findMany: async function (
            filter: FilterQuery<any> = {},
            sort: FilterQuery<any> = {},
            limit: number = 50,
            page: number = 1,
        ) {
            function dateToFilter(k: string, v: Date): any {
                switch (k) {
                    case 'purchasedAfter':
                        return ['purchaseDate', { $gt: v.toISOString() }];
                    case 'updatedAfter':
                        return ['lastChanged', { $gt: v.toISOString() }];
                    case 'shouldShipBy':
                        return ['latestShipDate', { $lt: v.toISOString() }];
                }
            }

            const mongoFilter = _.fromPairs(
                Object.entries(filter)
                    .filter(([k, v]) => !!v)
                    .map(([k, v]) =>
                        v instanceof Date
                            ? dateToFilter(k, v)
                            : Array.isArray(v)
                            ? [k, { $in: v }]
                            : [k, v],
                    ),
            );

            return (
                await (await collection())
                    .find(mongoFilter)
                    .project({ _id: 0 })
                    .sort(sort)
                    .skip(limit * (page - 1))
                    .limit(limit)
                    .toArray()
            ).map(mapDocumentFromDb);
        },

        findOneAndUpdate: async function (
            rc: IRequestContext,
            filter: FilterQuery<any> = {},
        ) {
            const originalDocument = await this.findOne(filter);

            return {
                originalDocument: _.cloneDeep(originalDocument),
                commit: async function (documentUpdate: Partial<T>) {
                    if (originalDocument) delete originalDocument['changelogs'];
                    delete documentUpdate['changelogs'];

                    const serializedChanges = serialize<T>(documentUpdate);
                    const serializedOriginal = serialize<T>(
                        originalDocument as T,
                    );
                    const changelog = createChangelog(
                        rc,
                        serializedOriginal,
                        serializedChanges,
                    );

                    const updateQuery: UpdateQuery<any> = {
                        $set: serializedChanges,
                        $push: {
                            changelogs: changelog as IChangelog | undefined,
                        } as PushOperator<any>,
                    };

                    const result = await (await collection()).findOneAndUpdate(
                        filter,
                        updateQuery,
                        {
                            projection: { _id: 0 },
                            returnOriginal: false,
                        },
                    );

                    if (!result.value)
                        throw new Error(
                            "Couldn't update" +
                                documentName +
                                (filter._id ? ` #${filter._id}` : ''),
                        );

                    return mapDocumentFromDb(result.value);
                },
            };
        },

        findManySubdocuments: async function <U>(
            subdocumentPath: string,
            filterParent: FilterQuery<any> = {},
            filterSubdocuments: FilterQuery<any> = {},
        ) {
            const subdocumentMatch = _.mapKeys(
                filterSubdocuments,
                (v, k) => `${subdocumentPath}.${k}`,
            );

            return (
                await (await collection())
                    .aggregate([
                        { $match: filterParent },
                        { $unwind: '$' + subdocumentPath },
                        { $match: subdocumentMatch },
                        // TODO in the future
                        // { $sort: { [`${subdocumentPath}._id`]: 1 } },
                        { $project: { [subdocumentPath]: 1 } },
                    ])
                    .toArray()
            )
                .map(mapDocumentFromDb)
                .map((doc: T) => doc[subdocumentPath] as U);
        },

        findOneSubdocument: async function <U>(
            subdocumentPath: string,
            filterParent: FilterQuery<any> = {},
            filterSubdocuments?: FilterQuery<any>,
        ) {
            const [subdoc] = await this.findManySubdocuments(
                subdocumentPath,
                filterParent,
                filterSubdocuments,
            );
            if (!subdoc)
                throw new Error(
                    'Resource' +
                        (filterSubdocuments?.id
                            ? ` #${filterSubdocuments?.id}`
                            : '') +
                        " doesn't exist on " +
                        documentName +
                        (filterParent._id ? ` #${filterParent._id}` : ''),
                );
            return subdoc as U;
        },

        findOneSubdocumentAndUpdate: async function <U>(
            rc: IRequestContext,
            filterParent: FilterQuery<any>,
            subdocumentPath: string,
            subdocumentPredicate: (document: U) => boolean,
        ) {
            const { originalDocument, commit } = await this.findOneAndUpdate(
                rc,
                filterParent,
            );

            const subdocumentReference: U = originalDocument[
                subdocumentPath
            ].find(subdocumentPredicate);

            if (!subdocumentReference)
                throw new Error(
                    "Resource doesn't exist on " +
                        documentName +
                        (filterParent._id ? ` #${filterParent._id}` : ''),
                );

            return {
                documentReference: originalDocument,
                subdocumentReference,
                commit,
            };
        },
    };
}
