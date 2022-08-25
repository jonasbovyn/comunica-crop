import { BindingsFactory } from '@comunica/bindings-factory';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { IJoinEntry, MetadataBindings } from '@comunica/types';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { IdpFindPlan } from '../../../lib/crop/idp/IdpFindPlan';

const BF = new BindingsFactory();
const DF = new DataFactory();

describe('IdpFindPlan', () => {
  let planFinder: IdpFindPlan;

  beforeEach(async() => {
    const entries: IJoinEntry[] = [
      {
        output: {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.literal('a1') ],
              [ DF.variable('b'), DF.literal('b1') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('a2') ],
              [ DF.variable('b'), DF.literal('b2') ],
            ]),
          ]),
          metadata: () => Promise.resolve(
            {
              cardinality: { type: 'estimate', value: 4 },
              pageSize: 100,
              requestTime: 10,
              canContainUndefs: false,
              variables: [ DF.variable('a'), DF.variable('b') ],
            },
          ),
          type: 'bindings',
        },
        operation: <any> {},
      },
      {
        output: {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.literal('a1') ],
              [ DF.variable('c'), DF.literal('c1') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('a2') ],
              [ DF.variable('c'), DF.literal('c2') ],
            ]),
          ]),
          metadata: () => Promise.resolve(
            {
              cardinality: { type: 'estimate', value: 5 },
              pageSize: 100,
              requestTime: 20,
              canContainUndefs: false,
              variables: [ DF.variable('a'), DF.variable('c') ],
            },
          ),
          type: 'bindings',
        },
        operation: <any> {},
      },
      {
        output: {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.literal('a1') ],
              [ DF.variable('b'), DF.literal('b1') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('a2') ],
              [ DF.variable('b'), DF.literal('b2') ],
            ]),
          ]),
          metadata: () => Promise.resolve(
            {
              cardinality: { type: 'estimate', value: 2 },
              pageSize: undefined,
              requestTime: 30,
              canContainUndefs: false,
              variables: [ DF.variable('a'), DF.variable('b') ],
            },
          ),
          type: 'bindings',
        },
        operation: <any> {},
      },
      {
        output: {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.literal('a1') ],
              [ DF.variable('b'), DF.literal('b1') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('a2') ],
              [ DF.variable('b'), DF.literal('b2') ],
            ]),
          ]),
          metadata: () => Promise.resolve(
            {
              cardinality: { type: 'estimate', value: 8 },
              pageSize: 200,
              requestTime: 30,
              canContainUndefs: false,
              variables: [ DF.variable('a'), DF.variable('b') ],
            },
          ),
          type: 'bindings',
        },
        operation: <any> {},
      },
      {
        output: {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.literal('a1') ],
              [ DF.variable('b'), DF.literal('b1') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('a2') ],
              [ DF.variable('b'), DF.literal('b2') ],
            ]),
          ]),
          metadata: () => Promise.resolve(
            {
              cardinality: { type: 'estimate', value: 7 },
              pageSize: 150,
              requestTime: 30,
              canContainUndefs: false,
              variables: [ DF.variable('a'), DF.variable('b') ],
            },
          ),
          type: 'bindings',
        },
        operation: <any> {},
      },
    ];
    const metadatas: MetadataBindings[] = await ActorRdfJoin.getMetadatas(entries);

    planFinder = new IdpFindPlan(entries, metadatas);
  });

  it('should be a function', () => {
    expect(IdpFindPlan).toBeInstanceOf(Function);
  });

  describe('findPlan', () => {
    it('should find an optimal plan', () => {
      const optimalPlans = planFinder.findPlan(3, 5);
      expect(optimalPlans.length).toEqual(1);
    });
  });

  function createEntries(arr: number[][]): IJoinEntry[] {
    const result: IJoinEntry[] = [];

    for (const entry of arr) {
      const joinEntry: IJoinEntry =
      {
        output: {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.literal('a1') ],
              [ DF.variable('b'), DF.literal('b1') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('a2') ],
              [ DF.variable('b'), DF.literal('b2') ],
            ]),
          ]),
          metadata: () => Promise.resolve(
            {
              cardinality: { type: 'exact', value: entry[0] },
              pageSize: entry[1] === -1 ? undefined : entry[1],
              requestTime: 10,
              canContainUndefs: false,
              variables: [ DF.variable('a'), DF.variable('b') ],
            },
          ),
          type: 'bindings',
        },
        operation: <any> {},
      };
      result.push(joinEntry);
    }

    return result;
  }

  it('specificTest', async() => {
    const entries = createEntries([[ 51, -1 ], [ 219, -1 ], [ 1, -1 ]]);
    const metadatas: MetadataBindings[] = await ActorRdfJoin.getMetadatas(entries);
    const planFinder2 = new IdpFindPlan(entries, metadatas);
    const optimalPLans = planFinder2.findPlan(4, 5);
    expect(optimalPLans.length).toEqual(1);
  });
});

