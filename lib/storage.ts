import {
    FilterQuery,
    IndexSpecification,
    UpdateQuery,
    PushOperator,
} from 'mongodb';
import Joi, { JoiObject } from 'typesafe-joi';
import _ from 'lodash';

import { db } from './db-connector';
import { convertObjectWithPattern } from 'expando-convert-pattern';
import { MoneySchema } from 'expando-money';
import { IChangelog } from '../schemas/changelog';
import { IRequestContext } from 'expando-request-context';

import { diff } from 'deep-diff';

interface Document {
    [key: string]: any;
    changelogs?: any;
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
    keepChangelog: boolean,
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
                allowUnknown: false,
                stripUnknown: true,
            },
        );

        if (error)
            throw new Error(
                `Error validating '${documentName}': ${error.message}`,
            );

        return value;
    };

    /**
     * Maps document from application to format that is valid for the database.
     * It is used mostly for conversion of object abstractions, etc
     */
    const serializeDocumentToDb = (doc: any): T => {
        const mappedDoc = convertObjectWithPattern<T>(
            doc,
            Object.keys(MoneySchema.describe().children),
            (moneyObject) => JSON.parse(JSON.stringify(moneyObject)),
        );

        return mappedDoc;
    };

    ensureCollection().then(() => ensureIndexes(indexes));

    return {
        insertOne: async function (rc: IRequestContext, document: any) {
            validate(document);

            const result = await (await collection()).insertOne(
                Object.assign(
                    serializeDocumentToDb(document),
                    keepChangelog
                        ? {
                              // add a changelog with empty changes to track
                              // where the document came from in the first place
                              changelogs: [createChangelog(rc, {}, {})],
                          }
                        : {},
                ),
            );

            return _.omit(result.ops[0], '_id');
        },

        findOne: async function (filter: FilterQuery<any> = {}) {
            const foundDocument = await (await collection()).findOne(filter, {
                projection: { _id: 0 },
            });
            if (!foundDocument)
                throw new Error(documentName + " doesn't exist");
            return foundDocument;
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
                        return ['purchaseDate', { $gt: v }];
                    case 'updatedAfter':
                        return ['lastChanged', { $gt: v }];
                    case 'shouldShipBy':
                        return ['latestShipDate', { $lt: v }];
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

            return await (await collection())
                .find(mongoFilter)
                .project({ _id: 0 })
                .sort(sort)
                .skip(limit * (page - 1))
                .limit(limit)
                .toArray();
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

                    const serializedChanges = serializeDocumentToDb(
                        documentUpdate,
                    );

                    const changelogUpdateQuery = (function () {
                        if (!keepChangelog) return null;

                        const serializedOriginal = serializeDocumentToDb(
                            originalDocument as T,
                        );
                        const changelog = createChangelog(
                            rc,
                            serializedOriginal,
                            serializedChanges,
                        );
                        return {
                            $push: {
                                changelogs: changelog as IChangelog | undefined,
                            } as PushOperator<any>,
                        };
                    })();

                    const updateQuery: UpdateQuery<any> = {
                        $set: serializedChanges,
                        ...changelogUpdateQuery,
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
                        throw new Error("Couldn't update" + documentName);

                    return result.value;
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
            ).map((doc: T) => doc[subdocumentPath] as U);
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
                throw new Error("Resource doesn't exist on " + documentName);
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
            ]?.find(subdocumentPredicate);

            if (!subdocumentReference)
                throw new Error("Resource doesn't exist on " + documentName);

            return {
                documentReference: originalDocument,
                subdocumentReference,
                commitAndReturnSubdocument: async function (
                    documentUpdate: Partial<T>,
                ): Promise<U> {
                    return (await commit(documentUpdate))[
                        subdocumentPath
                    ]?.find(subdocumentPredicate);
                },
            };
        },
    };
}
