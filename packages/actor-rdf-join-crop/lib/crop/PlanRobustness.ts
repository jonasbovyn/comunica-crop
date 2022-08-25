import type { IJoinEntry, MetadataBindings } from '@comunica/types';
import type { IdpQueryPlan } from './idp/IdpQueryPlan';
import { IdpQueryLeaf, IdpQueryNode } from './idp/IdpQueryPlan';
import type { QueryOperator } from './QueryOperator';
import type { IQueryLeaf, IQueryNode, IQueryPlan } from './QueryPlan';

class IdpQueryNode1 extends IdpQueryNode {
  // Same implementation as original
  protected estimateCardinality(entries: IJoinEntry[], metadatas: MetadataBindings[]): number {
    return Math.min(
      this.left.cardinality,
      this.right.cardinality,
    );
  }
}

class IdpQueryNode2 extends IdpQueryNode {
  protected estimateCardinality(entries: IJoinEntry[], metadatas: MetadataBindings[]): number {
    return this.left.cardinality + this.right.cardinality;
  }
}

class IdpQueryNode3 extends IdpQueryNode {
  protected estimateCardinality(entries: IJoinEntry[], metadatas: MetadataBindings[]): number {
    return Math.max(
      this.left.cardinality,
      this.right.cardinality,
    );
  }
}

class IdpQueryNode4 extends IdpQueryNode {
  protected estimateCardinality(entries: IJoinEntry[], metadatas: MetadataBindings[]): number {
    if (this.right.cardinality === 0 || this.left.cardinality === 0) {
      // Side case not found in the paper, but is in the source code of CROP
      return 0;
    }
    return Math.max(
      this.left.cardinality / this.right.cardinality,
      this.right.cardinality / this.left.cardinality,
    );
  }
}

function createAlternativeNode(alternative: number, left: IdpQueryPlan, op: QueryOperator, right: IdpQueryPlan,
  entries: IJoinEntry[], metadatas: MetadataBindings[]): IdpQueryPlan {
  if (alternative === 1) {
    return new IdpQueryNode1(left, op, right, entries, metadatas);
  }
  if (alternative === 2) {
    return new IdpQueryNode2(left, op, right, entries, metadatas);
  }
  if (alternative === 3) {
    return new IdpQueryNode3(left, op, right, entries, metadatas);
  }

  return new IdpQueryNode4(left, op, right, entries, metadatas);
}

function reconstructPlan(plan: IQueryPlan, alternative: number,
  entries: IJoinEntry[], metadatas: MetadataBindings[]): IdpQueryPlan {
  if (plan.triples === 1) {
    // It is a leaf, cost is always 0
    return new IdpQueryLeaf((<IQueryLeaf>plan).entryIndex, entries, metadatas);
  }

  const node = <IQueryNode> plan;
  return createAlternativeNode(alternative,
    reconstructPlan(node.left, alternative, entries, metadatas),
    node.op,
    reconstructPlan(node.right, alternative, entries, metadatas),
    entries,
    metadatas);
}

function median(values: number[]): number {
  values.sort((val1, val2) => val1 - val2);

  const half = Math.floor(values.length / 2);

  if (values.length % 2) {
    return values[half];
  }

  return (values[half - 1] + values[half]) / 2;
}

/**
 *
 * @param plan assumed to be the bestCase plan
 * @param entries
 * @param metadatas
 */
export function getPlanRobustness(plan: IQueryPlan, entries: IJoinEntry[], metadatas: MetadataBindings[]): number {
  const bestCost = plan.cost;

  const costs = [];
  for (let i = 0; i < 4; i++) {
    const reconstructed = reconstructPlan(plan, i, entries, metadatas);
    costs.push(reconstructed.cost);
  }

  const averageCost = median(costs);
  // Robustness
  return bestCost / averageCost;
}

export function selectRobustPlan(plans: IQueryPlan[], robustnessThreshold: number, costThreshold: number,
  entries: IJoinEntry[], metadatas: MetadataBindings[]): IQueryPlan {

  let bestPlan = plans.reduce((prev, curr) => prev.cost < curr.cost ? prev : curr);
  if (getPlanRobustness(bestPlan, entries, metadatas) < robustnessThreshold && plans.length > 1) {
    let potentialPlans = plans.filter((plan, index) =>
      getPlanRobustness(plan, entries, metadatas) >= robustnessThreshold);

    if (potentialPlans.length === 0) {
      potentialPlans = plans.filter((plan, index) => plan !== bestPlan);
      // Fixme why is an alternative (higher cost) plan chosen without needing to be more robust??
    }

    const bestAlternative = potentialPlans.reduce((prev, curr) => prev.cost < curr.cost ? prev : curr);
    if (bestPlan.cost / bestAlternative.cost > costThreshold) {
      bestPlan = bestAlternative;
    }
  }

  return bestPlan;
}
