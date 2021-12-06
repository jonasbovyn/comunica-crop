import type { IActorQueryOperationTypedMediatedArgs } from '@comunica/bus-query-operation';
import {
  ActorQueryOperation,
  ActorQueryOperationTypedMediated,
} from '@comunica/bus-query-operation';
import type { IActorTest } from '@comunica/core';
import type {
  IQueryableResultBindings, IMetadata, IActionContext, IQueryableResult,
} from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { getTerms, getVariables, uniqTerms } from 'rdf-terms';
import { Algebra } from 'sparqlalgebrajs';
import { BindingsToQuadsIterator } from './BindingsToQuadsIterator';

/**
 * A comunica Construct Query Operation Actor.
 */
export class ActorQueryOperationConstruct extends ActorQueryOperationTypedMediated<Algebra.Construct> {
  public constructor(args: IActorQueryOperationTypedMediatedArgs) {
    super(args, 'construct');
  }

  /**
   * Find all variables in a list of triple patterns.
   * @param {Algebra.Pattern[]} patterns An array of triple patterns.
   * @return {RDF.Variable[]} The variables in the triple patterns.
   */
  public static getVariables(patterns: RDF.BaseQuad[]): RDF.Variable[] {
    return uniqTerms((<RDF.Variable[]> []).concat
      .apply([], patterns.map(pattern => getVariables(getTerms(pattern)))));
  }

  public async testOperation(operation: Algebra.Construct, context: IActionContext | undefined): Promise<IActorTest> {
    return true;
  }

  public async runOperation(operationOriginal: Algebra.Construct, context: IActionContext | undefined):
  Promise<IQueryableResult> {
    // Apply a projection on our CONSTRUCT variables first, as the query may contain other variables as well.
    const variables: RDF.Variable[] = ActorQueryOperationConstruct.getVariables(operationOriginal.template);
    const operation: Algebra.Operation = { type: Algebra.types.PROJECT, input: operationOriginal.input, variables };

    // Evaluate the input query
    const output: IQueryableResultBindings = ActorQueryOperation.getSafeBindings(
      await this.mediatorQueryOperation.mediate({ operation, context }),
    );

    // Construct triples using the result based on the pattern.
    const quadStream: AsyncIterator<RDF.Quad> = new BindingsToQuadsIterator(
      operationOriginal.template,
      output.bindingsStream,
    );

    // Let the final metadata contain the estimated number of triples
    const metadata: (() => Promise<IMetadata>) = () => output.metadata().then(meta => ({
      ...meta,
      cardinality: meta.cardinality * operationOriginal.template.length,
      canContainUndefs: false,
    }));

    return {
      metadata,
      quadStream,
      type: 'quads',
    };
  }
}
