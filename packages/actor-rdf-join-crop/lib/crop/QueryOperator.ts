export enum QueryOperator {
  NestedLoop,
  Hash
}

export const allIdpQueryOperators = [ QueryOperator.NestedLoop, QueryOperator.Hash ];

export const idpOperatorCost = new Map<number, number>([
  [ QueryOperator.NestedLoop, 0.001 ],
  [ QueryOperator.Hash, 0.001 ],
]);
