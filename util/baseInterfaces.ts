export type integer = number;

export interface Identifiable {
    _key?: string;
    _id?: string;
    _rev?: string;
}

export interface EdgesIdentifiable extends Identifiable {
    _from?: string;
    _to?: string;
}
