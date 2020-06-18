import * as Joi from 'typesafe-joi';
import _ from 'lodash';

import { ObjectId } from 'mongodb';

// -----------------------------------------------------------------------------
export const OrderDocumentSchema = Joi.object({
    id: Joi.object().type(ObjectId).required(),
    companyId: Joi.object().type(ObjectId).required(),

    channel: Joi.string().required(),
    channelOrderId: Joi.string().required(),

    fulfillmentService: Joi.string().valid('Seller', 'FBA').required(),

    purchaseDate: Joi.date().required(),
    selectedShipDate: Joi.date().optional(),
    latestShipDate: Joi.date().optional(),
    latestDeliveryDate: Joi.date().optional(),
    reservedUntilDate: Joi.date().optional(),

    status: Joi.string().valid(['Pending', 'Cancelled']).required(),
    statusDate: Joi.object().required(),

    fulfillmentStatus: Joi.string()
        .valid(['Unshipped', 'Shipped', 'Delivered', 'Rejected', 'Failed'])
        .required(),

    cancellationReason: Joi.string().optional(),

    totalItemsCount: Joi.number().required(),
    unshippedCount: Joi.number().required(),
    shippedCount: Joi.number().required(),
    deliveredCount: Joi.number().required(),
    rejectedCount: Joi.number().required(),
}).required();

export type IOrderDocument = Joi.Literal<typeof OrderDocumentSchema>;
