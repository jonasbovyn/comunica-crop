// eslint-disable-next-line eslint-comments/disable-enable-pair
/* eslint-disable no-bitwise, id-length*/
/* global BigInt */

/**
 * Returns the next k-subset in lexographical ordering
 * @param bound upper bound
 * @param set current set (k = length of set)
 */
export function kSubsetLexSuccessor(bound: number, set: number[]): number[] | undefined {
  // Donald L. Kreher en Douglas R. Stinson, “Combinatorial Algorithms: Generation, Enumeration and Search”,
  // CRC Press, 1999

  const k = set.length;
  let i = k - 1;
  while (i >= 0 && set[i] === bound - k + i) {
    i -= 1;
  }
  if (i === -1) {
    return undefined;
  }

  const buffer = set[i];
  for (let j = i; j < k; j += 1) {
    set[j] = buffer + j - i + 1;
  }
  return set;
}

// /**
//  * Same as kSubsetLexSuccessor, where each number now represents an indexed object of "collection" instead
//  * assumes "set" is ordered by the order of collection
//  * @param collection
//  * @param set
//  */
// export function kSubsetSuccessor<T>(collection: T[], set: T[]): T[] | undefined {
//   // convert set to indexes
//   for (let i = 0; i < set.length; i += 1) {
//
//   }
//
//   // convert back
// }

export function firstKSubset(k: number): number[] {
  return [ ...new Array(k).keys() ];
}

// Export function transform<T>(indices: number[], collection: T[]): T[] {
//   return indices.map(v => collection[v]);
// }

/**
 * Masks a set to a primitive bigint
 * @param set
 */
export function mask(set: number[]): bigint {
  let result = 0n;
  for (const n of set) {
    result |= 1n << BigInt(n);
  }
  return result;
}

export function maskWithCollection(collection: number[], indexes: number[]): bigint {
  let result = 0n;
  for (const index of indexes) {
    result |= 1n << BigInt(collection[index]);
  }
  return result;
}
