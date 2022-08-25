import { BindingsFactory } from '@comunica/bindings-factory';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { IJoinEntry, MetadataBindings } from '@comunica/types';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { IdpQueryLeaf, IdpQueryNode } from '../../../lib/crop/idp/IdpQueryPlan';
import { QueryOperator } from '../../../lib/crop/QueryOperator';

const BF = new BindingsFactory();
const DF = new DataFactory();

describe('IdpQueryPlan', () => {
  let entries: IJoinEntry[];
  let metadatas: MetadataBindings[];
  beforeEach(async() => {
    entries = [
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
    ];

    metadatas = await ActorRdfJoin.getMetadatas(entries);
  });

  describe('IdpQueryLeaf', () => {
    it('should be a function', () => {
      expect(IdpQueryLeaf).toBeInstanceOf(Function);
    });

    it('complies with the IIdpQueryPlan interface', () => {
      const leaf = new IdpQueryLeaf(0, entries, metadatas);
      expect(leaf.cost).toBe(0); // No computational join cost
      expect(leaf.triples).toBe(1);
      expect(leaf.height).toBe(1);
    });

    it('has the right cardinalities', () => {
      const leaf1 = new IdpQueryLeaf(0, entries, metadatas);
      expect(leaf1.cardinality).toBe(4);
      const leaf2 = new IdpQueryLeaf(1, entries, metadatas);
      expect(leaf2.cardinality).toBe(5);
      const leaf3 = new IdpQueryLeaf(2, entries, metadatas);
      expect(leaf3.cardinality).toBe(2);
    });
  });

  describe('IdpQueryNode', () => {
    interface INodeInfo {
      plan: IdpQueryNode;
      height: number;
      triples: number;
    }

    let nodeInfos: INodeInfo[];

    beforeEach(() => {
      const leaf1 = new IdpQueryLeaf(0, entries, metadatas);
      const leaf2 = new IdpQueryLeaf(1, entries, metadatas);
      const leaf3 = new IdpQueryLeaf(2, entries, metadatas);

      nodeInfos = [
        {
          plan: new IdpQueryNode(leaf1, QueryOperator.Hash, leaf2, entries, metadatas),
          height: 2,
          triples: 2,
        },
        {
          plan: new IdpQueryNode(leaf3, QueryOperator.Hash, leaf2, entries, metadatas),
          height: 2,
          triples: 2,
        },
        {
          plan:
            new IdpQueryNode(
              new IdpQueryNode(leaf1, QueryOperator.Hash, leaf2, entries, metadatas),
              QueryOperator.NestedLoop,
              leaf3,
              entries,
              metadatas,
            ),
          height: 3,
          triples: 3,
        },
        {
          plan:
            new IdpQueryNode(
              new IdpQueryNode(leaf1, QueryOperator.NestedLoop, leaf2, entries, metadatas),
              QueryOperator.Hash,
              leaf3,
              entries,
              metadatas,
            ),
          height: 3,
          triples: 3,
        },
        {
          plan:
            new IdpQueryNode(
              leaf3,
              QueryOperator.Hash,
              new IdpQueryNode(leaf1, QueryOperator.NestedLoop, leaf2, entries, metadatas),
              entries,
              metadatas,
            ),
          height: 3,
          triples: 3,
        },
      ];
    });

    it('should be a function', () => {
      expect(IdpQueryNode).toBeInstanceOf(Function);
    });

    it('describes correct query plans', () => {
      for (const nodeInfo of nodeInfos) {
        // Has the correct heights
        expect(nodeInfo.plan.height).toBe(nodeInfo.height);

        // Has the correct triples
        expect(nodeInfo.plan.triples).toBe(nodeInfo.triples);

        // Has a non-null cost
        expect(nodeInfo.plan.cost).not.toBe(0);

        // eslint-disable-next-line no-console
        console.log(nodeInfo.plan.cost);

        // Has an estimated cardinality
        expect(nodeInfo.plan.cardinality).toBeDefined();

        // eslint-disable-next-line no-console
        console.log(nodeInfo.plan.cardinality);
      }
    });
  });
});

