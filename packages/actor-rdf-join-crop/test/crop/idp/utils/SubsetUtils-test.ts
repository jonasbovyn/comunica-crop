import { kSubsetLexSuccessor, firstKSubset, mask, maskWithCollection }
  from '../../../../lib/crop/idp/utils/SubsetUtils';

describe('SubsetUtils', () => {
  describe('firstKSubset', () => {
    it('should be a function', () => {
      expect(firstKSubset).toBeInstanceOf(Function);
    });

    it('should return the first k-subset of a set', () => {
      expect(firstKSubset(0)).toEqual([ ]);
      expect(firstKSubset(1)).toEqual([ 0 ]);
      expect(firstKSubset(4)).toEqual([ 0, 1, 2, 3 ]);
    });
  });

  describe('kSubsetLexSuccessor', () => {
    it('should be a function', () => {
      expect(kSubsetLexSuccessor).toBeInstanceOf(Function);
    });

    it('should iterate over range if k = 1', () => {
      let set: number[] | undefined = firstKSubset(1); // [ 0 ]
      const bound = 5;

      for (let i = 1; i < bound; i++) {
        set = kSubsetLexSuccessor(bound, set!);
        expect(set).toEqual([ i ]);
      }
    });

    it('should return the correct subset', () => {
      let set: number[] | undefined = firstKSubset(3); // [ 0 ]
      const n = 5;

      // Expected k-subsets for n=5 & k=3
      const expectedSets = [
        [ 0, 1, 2 ],
        [ 0, 1, 3 ],
        [ 0, 1, 4 ],
        [ 0, 2, 3 ],
        [ 0, 2, 4 ],
        [ 0, 3, 4 ],
        [ 1, 2, 3 ],
        [ 1, 2, 4 ],
        [ 1, 3, 4 ],
        [ 2, 3, 4 ],
      ];

      for (const expectedSet of expectedSets) {
        expect(set).toEqual(expectedSet);
        set = kSubsetLexSuccessor(n, set!);
      }

      expect(set).toBeUndefined();
    });

    it('should be undefined when it iterated over all subsets', () => {
      function setAfterXiterations(n: number, k: number, iterations: number): number[] | undefined {
        let set: number[] | undefined = firstKSubset(k); // [ 0 ]
        for (let i = 0; i < iterations; i++) {
          set = kSubsetLexSuccessor(n, set!);
        }
        return set;
      }

      const n = 5;
      const iterationsForK = [ 1, 5, 10, 10, 5, 1 ];
      for (const [ k, iterations ] of iterationsForK.entries()) {
        expect(setAfterXiterations(n, k, iterations)).toBeUndefined();
      }
    });
  });

  describe('mask', () => {
    it('should be a function', () => {
      expect(mask).toBeInstanceOf(Function);
    });

    it('should mask a numbered set to a biginteger primitive', () => {
      expect(mask([ ])).toBe(0n);
      expect(mask([ 0 ])).toBe(1n);
      expect(mask([ 0, 1, 2 ])).toBe(0b111n);
      expect(mask([ 0, 1, 3 ])).toBe(0b1011n);
      expect(mask([ 0, 3, 8, 9 ])).toBe(0b11_0000_1001n);
    });
  });

  describe('maskWithCollection', () => {
    it('should be a function', () => {
      expect(maskWithCollection).toBeInstanceOf(Function);
    });

    it('should mask an index-set with values from a collection', () => {
      const c = [ 3, 2, 1, 4, 8, 5, 6, 7, 0 ];

      expect(maskWithCollection(c, [ ])).toBe(0n);
      expect(maskWithCollection(c, [ 0 ])).toBe(0b1000n);
      expect(maskWithCollection(c, [ 0, 1, 2 ])).toBe(0b1110n);
      expect(maskWithCollection(c, [ 0, 1, 3 ])).toBe(0b1_1100n);
      expect(maskWithCollection(c, [ 0, 3, 7, 8 ])).toBe(0b1001_1001n);
    });
  });
});
