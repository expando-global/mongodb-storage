import { ObjectId } from 'mongodb';
import { IRequestContext } from '../schemas/request-context';

export function makeMockRequestContext(companyId?: ObjectId): IRequestContext {
    return {
        companyId: companyId || new ObjectId(),
        ip: '192.168.0.66',
        token: '******X-9a8dF2',
        endpoint: 'POST http://localhost:3333/resources/resourceId',
    };
}
