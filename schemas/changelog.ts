import * as Joi from 'typesafe-joi';
import { Diff } from 'deep-diff';

export const ChangelogSchema = Joi.object({
    token: Joi.string().required(),
    ip: Joi.string().required(),
    endpoint: Joi.string().required(),
    timestamp: Joi.date().required(),
    changes: Joi.array()
        // @ts-ignore
        .items(Joi.object({}) as Joi.Cast.Object<Diff>)
        .required(),
}).required();
export type IChangelog = Joi.Literal<typeof ChangelogSchema>;
