import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { IJoinEntry, MetadataBindings } from '@comunica/types';
import { QueryOperator, idpOperatorCost } from '../QueryOperator';
import type { IQueryLeaf, IQueryNode, IQueryPlan } from '../QueryPlan';

export interface IIdpQueryPlan {
  cost: number;
  cardinality: number;
  triples: number;
  height: number;
  variables: Set<string>;

  creationIndex: number;
  compare: (plan2: IIdpQueryPlan) => number;
}

export type IdpQueryPlan = IIdpQueryPlan & IQueryPlan;

export class IdpQueryLeaf implements IIdpQueryPlan, IQueryLeaf {
  public cardinality: number;
  public cost: number;
  public readonly variables: Set<string>;

  public constructor(
    public readonly entryIndex: number,
    entries: IJoinEntry[],
    private readonly metadatas: MetadataBindings[],

    public readonly creationIndex = 0,
  ) {
    this.cardinality = ActorRdfJoin.getCardinality(metadatas[this.entryIndex]).value;
    this.variables = new Set(metadatas[this.entryIndex].variables.map(variable => variable.value));
    this.cost = this.cardinality;
  }

  public requestCost(): number {
    const pageSize = this.metadatas[this.entryIndex].pageSize;
    const requests = pageSize === undefined ? 1 : this.cardinality / pageSize;

    return Math.max(requests, 1);
  }

  public readonly triples = 1;
  public readonly height = 0;

  public compare(plan2: IIdpQueryPlan): number {
    if (this.cost === plan2.cost) {
      return this.creationIndex - plan2.creationIndex;
    }
    return this.cost - plan2.cost;
  }
}

export class IdpQueryNode implements IIdpQueryPlan, IQueryNode {
  public cardinality: number;
  public cost: number;
  public triples: number;
  public height: number;
  public readonly variables: Set<string>;

  public constructor(
    public readonly left: IdpQueryPlan,
    public readonly op: QueryOperator,
    public readonly right: IdpQueryPlan,
    entries: IJoinEntry[],
    metadatas: MetadataBindings[],

    public readonly creationIndex = 0,
  ) {
    this.height = Math.max(left.height, right.height) + 1;
    this.triples = left.triples + right.triples;
    this.variables = new Set([ ...left.variables, ...right.variables ]);
    this.cardinality = this.estimateCardinality(entries, metadatas);
    this.cost = this.calculateCost(entries, metadatas);
  }

  private processCost(entries: IJoinEntry[], metadatas: MetadataBindings[]): number {
    const opCost = idpOperatorCost.get(this.op)!;

    if (this.op === QueryOperator.Hash) {
      return opCost * this.cardinality;
    }
    // If (this.op === QueryOperator.NestedLoop) {
    return opCost * (this.cardinality + this.right.cardinality);
    // }
  }

  private requestCost(entries: IJoinEntry[], metadatas: MetadataBindings[]): number {
    let totalRequestCost = 0;
    if (this.left.triples === 1) {
      // Request needs to happen because it will be a leaf
      totalRequestCost += (<IdpQueryLeaf> this.left).requestCost();
    }

    if (this.op === QueryOperator.Hash) {
      if (this.right.triples === 1) {
        // Request needs to happen because it will be a leaf
        totalRequestCost += (<IdpQueryLeaf> this.right).requestCost();
      }
    } else {
      // If (this.op === QueryOperator.NestedLoop)

      const heightFactor = Math.max(1, 4 * this.height);

      const pageSize = metadatas[(<IdpQueryLeaf>(this.right)).entryIndex].pageSize;
      const requests = pageSize === undefined ? 1 : this.cardinality / pageSize;

      // Does not align with cost model from paper, but does align with the actual real nLDE implementation
      totalRequestCost += Math.max(this.left.cardinality, 10 * requests, 1) / heightFactor;
    }

    return totalRequestCost;
  }

  private calculateCost(entries: IJoinEntry[], metadatas: MetadataBindings[]): number {
    return this.processCost(entries, metadatas) + this.requestCost(entries, metadatas) +
      (this.left.triples === 1 ? 0 : this.left.cost) + (this.right.triples === 1 ? 0 : this.right.cost);
  }

  protected estimateCardinality(entries: IJoinEntry[], metadatas: MetadataBindings[]): number {
    return Math.min(
      this.left.cardinality,
      this.right.cardinality,
    );
  }

  public compare(plan2: IIdpQueryPlan): number {
    if (this.cost === plan2.cost) {
      return this.creationIndex - plan2.creationIndex;
    }
    return this.cost - plan2.cost;
  }
}
