// eslint-disable-next-line eslint-comments/disable-enable-pair
/* eslint-disable no-bitwise, id-length*/
/* global BigInt */
import type { IJoinEntry, MetadataBindings } from '@comunica/types';
import { Heap } from 'heap-js';
import { ActorRdfJoinCrop } from '../../ActorRdfJoinCrop';
import { allIdpQueryOperators, QueryOperator } from '../QueryOperator';
import type { IdpQueryPlan } from './IdpQueryPlan';
import { IdpQueryLeaf, IdpQueryNode } from './IdpQueryPlan';
import { firstKSubset, kSubsetLexSuccessor, mask, maskWithCollection } from './utils/SubsetUtils';

export class IdpFindPlan {
  public constructor(
    private readonly entries: IJoinEntry[],
    private readonly metadatas: MetadataBindings[],
    private currentNodeIndex = 0,
  ) {
  }

  /**
   * Filters irrelevant query plans from the optimal plans
   * @param key key of the optimal plan to prune
   * @param optPlans optimal plans
   * @param t top t plans to be kept in optPlans
   * @private
   */
  private prunePlans(key: bigint, optPlans: Map<bigint, IdpQueryPlan[]>, t: number): void {
    const planCostComparator = (a: IdpQueryPlan, b: IdpQueryPlan): number => a.compare(b);
    const heap = new Heap(planCostComparator);

    for (const plan of optPlans.get(key)!) {
      heap.add(plan);
    }

    const bestPlans: IdpQueryPlan[] = [];
    let i = 0;
    while (i < t && !heap.isEmpty()) {
      bestPlans.push(heap.pop()!);
      i++;
    }

    optPlans.set(key, bestPlans);
  }

  /**
   * Returns all possible joins of different plans for 2 sets of entries
   * @param plans1
   * @param plans2
   * @private
   */
  private joinPlans(plans1: IdpQueryPlan[], plans2: IdpQueryPlan[]): IdpQueryPlan[] {
    if (plans1 === undefined || plans2 === undefined) {
      return [];
    }

    // In current implementation, only consider hash & nestedLoop joins
    const joinedPlans = [];
    for (const plan1 of plans1) {
      for (const plan2 of plans2) {
        // If the intersection of variables is empty, it is not a useful join operation
        if ([ ...plan1.variables ].every(v => !plan2.variables.has(v))) {
          continue;
        }

        for (const operator of allIdpQueryOperators) {
          if (operator === QueryOperator.NestedLoop) {
            // Quote from paper: In this work, we restrict the inner plan
            // in the NLJs to be triple patterns only, i.e. |P2| = 1
            if (plan2.triples === 1) {
              joinedPlans.push(
                new IdpQueryNode(plan1, operator, plan2, this.entries, this.metadatas, this.currentNodeIndex++),
              );
            }
            if (plan1.triples === 1) {
              joinedPlans.push(
                new IdpQueryNode(plan2, operator, plan1, this.entries, this.metadatas, this.currentNodeIndex++),
              );
            }
          } else {
            joinedPlans.push(
              new IdpQueryNode(plan1, operator, plan2, this.entries, this.metadatas, this.currentNodeIndex++),
            );
          }
        }
      }
    }

    return joinedPlans;
  }

  /**
   * Finds the best plan with IDP-standard-bestPlan, might change to bestRow later (?)
   * @param k
   * @param t
   */
  public findPlan(k: number, t: number): IdpQueryPlan[] {
    global.gc();
    const initialMem = process.memoryUsage().heapUsed;
    let highestMem = initialMem;

    this.currentNodeIndex = 0;

    // OptPlans is indexed with bitmasks of the selection
    const optPlans = new Map<bigint, IdpQueryPlan[]>();
    for (let i = 0; i < this.entries.length; i++) {
      // Set the access plans
      optPlans.set(1n << BigInt(i), [ new IdpQueryLeaf(i, this.entries, this.metadatas, this.currentNodeIndex++) ]);

      // Since there is only one access plan, there is no need to call prunePlans yet
    }

    let nextSymbol = this.entries.length;
    let toDo = [ ...new Array(this.entries.length).keys() ];

    while (toDo.length > 1) {
      const todoConst = toDo;

      k = Math.min(k, toDo.length);
      for (let i = 2; i <= k; i += 1) {
        // Represents the indexes in "to-Do" that will be considered right now
        let indicesS: number[] | undefined = firstKSubset(i);

        // Iterate over all K-subsets, where K is the current "i"
        while (indicesS !== undefined) {
          const S = indicesS.map(v => todoConst[v]);
          const maskS = mask(S);

          if (!optPlans.has(maskS)) {
            optPlans.set(maskS, []);

            // Indices of the O set relative to S
            let maskOtoS = 1;

            // "- 1" to not include itself
            // Const upperbound = (1 << i) - 1;

            // Reflexivity, set the highest bit to always 0 because it will be included in the negation
            const upperbound = 1 << (i - 1);
            // Iterate over all subsets of S
            while (maskOtoS < upperbound) {
              // Mask of O relative to to-Do
              let maskO = 0n;
              for (let j = 0; j < i; j++) {
                if ((maskOtoS & (1 << j)) !== 0) {
                  maskO |= 1n << BigInt(S[j]);
                }
              }

              const newPlans = this.joinPlans(optPlans.get(maskO)!, optPlans.get(maskS - maskO)!);
              if (newPlans.length > 0) {
                optPlans.get(maskS)!.push(...newPlans);
                this.prunePlans(maskS, optPlans, newPlans[0].triples === 2 ? 1 : t);
              }

              maskOtoS += 1;
            }
          }

          indicesS = kSubsetLexSuccessor(toDo.length, indicesS);
        }
      }

      // Global.gc();
      // Mem usage is at its peak here
      const memUsage = process.memoryUsage();
      if (memUsage.heapUsed > highestMem) {
        highestMem = memUsage.heapUsed;
      }

      // Find the best plan with length K
      let indicesV: number[] | undefined = firstKSubset(k);
      let bestV: number[];
      let bestVmask: bigint;
      let bestPlans: IdpQueryPlan[];
      let bestPlan: IdpQueryPlan | undefined;
      while (indicesV !== undefined) {
        const maskV = maskWithCollection(toDo, indicesV);

        for (const plan of optPlans.get(maskV)!) {
          if (bestPlan === undefined || plan.compare(bestPlan) < 0) {
            bestV = [ ...indicesV ].map(v => todoConst[v]);
            bestVmask = maskV;
            bestPlans = optPlans.get(maskV)!;
            bestPlan = plan;
          }
        }

        indicesV = kSubsetLexSuccessor(toDo.length, indicesV);
      }

      for (const key of optPlans.keys()) {
        if ((key & bestVmask!) !== 0n) {
          optPlans.delete(key);
        }
      }

      const Vset = new Set(bestV!);
      toDo = toDo.filter(v => !Vset.has(v));
      toDo.push(nextSymbol);

      // Trim down to only the best plan (and neglect the top-t), according to original IDP source.
      // Don't do that if it's the last iteration, to retain the alternatives
      optPlans.set(1n << BigInt(nextSymbol), toDo.length > 1 ? [ bestPlan! ] : bestPlans!);

      nextSymbol += 1;
    }

    ActorRdfJoinCrop.benchmark('crop-memory', highestMem - initialMem);

    const key = [ ...optPlans.keys() ][0];
    this.prunePlans(key, optPlans, t);
    return optPlans.get(key)!;
  }
}
