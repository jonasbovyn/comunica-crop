// Interfaces shared across both JS & Wasm implementations, with bare minimum fields for execution of them
import { QueryOperator } from './QueryOperator';

export type IQueryPlan = IQueryLeaf | IQueryNode;

interface IQueryPlanCommon {
  cost: number;
  triples: number;
}

export interface IQueryLeaf extends IQueryPlanCommon {
  entryIndex: number;
}

export interface IQueryNode extends IQueryPlanCommon {
  left: IQueryPlan;
  op: QueryOperator;
  right: IQueryPlan;
}

function queryLeafToString(leaf: IQueryLeaf): string {
  return `${leaf.entryIndex}`;
}

function queryNodeString(plan: IQueryNode): string {
  return `[${queryPlanToString(plan.left)} <${plan.op === QueryOperator.Hash ? 'HASH' : 'NESTEDLOOP'}> ${queryPlanToString(plan.right)}]`;
}

export function queryPlanToString(plan: IQueryPlan): string {
  if (plan.triples === 1) {
    return queryLeafToString(<IQueryLeaf>plan);
  }
  return queryNodeString(<IQueryNode>plan);
}

