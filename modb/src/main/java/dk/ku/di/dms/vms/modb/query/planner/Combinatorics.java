package dk.ku.di.dms.vms.modb.query.planner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.Logger.Level.INFO;

public final class Combinatorics {

    private Combinatorics(){}

    private static final System.Logger LOGGER = System.getLogger(Combinatorics.class.getName());

    private static List<int[]> getCombinationsFor2SizeColumnList(int[] filterColumns ){
        LOGGER.log(INFO,
                "Combinatorics: getCombinationsFor2SizeColumnList called with filterColumns="
                        + Arrays.toString(filterColumns));

        int[] arr0 = { filterColumns[0] };
        int[] arr1 = { filterColumns[1] };

        List<int[]> res = Arrays.asList(arr0, arr1);

        LOGGER.log(INFO, "Combinatorics: len==2 returning combinations.size=" + res.size());
        return res;
    }

    private static List<int[]> getCombinationsFor3SizeColumnList( int[] filterColumns ){
        LOGGER.log(INFO,
                "Combinatorics: getCombinationsFor3SizeColumnList called with filterColumns="
                        + Arrays.toString(filterColumns));

        int[] arr0 = { filterColumns[0] };
        int[] arr1 = { filterColumns[1] };
        int[] arr2 = { filterColumns[2] };
        int[] arr4 = { filterColumns[0], filterColumns[1] };
        int[] arr5 = { filterColumns[0], filterColumns[2] };
        int[] arr6 = { filterColumns[1], filterColumns[2] };

        List<int[]> res = Arrays.asList( arr0, arr1, arr2, arr4, arr5, arr6 );

        LOGGER.log(INFO, "Combinatorics: len==3 returning combinations.size=" + res.size());
        return res;
    }

    private static List<int[]> getCombinationsFor4SizeColumnList( int[] filterColumns ){
        LOGGER.log(INFO,
                "Combinatorics: getCombinationsFor4SizeColumnList called with filterColumns="
                        + Arrays.toString(filterColumns));

        int[] arr0 = { filterColumns[0] };
        int[] arr1 = { filterColumns[1] };
        int[] arr2 = { filterColumns[2] };
        int[] arr3 = { filterColumns[3] };

        int[] arr4 = { filterColumns[0], filterColumns[1] };
        int[] arr5 = { filterColumns[0], filterColumns[2] };
        int[] arr6 = { filterColumns[0], filterColumns[3] };

        int[] arr7 = { filterColumns[1], filterColumns[2] };
        int[] arr8 = { filterColumns[1], filterColumns[3] };

        int[] arr9 = { filterColumns[2], filterColumns[3] };

        List<int[]> res = Arrays.asList( arr0, arr1, arr2, arr3, arr4, arr5, arr6, arr7, arr8, arr9 );

        LOGGER.log(INFO, "Combinatorics: len==4 returning combinations.size=" + res.size());
        return res;
    }

    // TODO later get metadata to know whether such a column has an index, so it can be pruned from this search
    // TODO a column can appear more than once. make it sure it appears only once
    public static List<int[]> getAllPossibleColumnCombinations( int[] filterColumns ){

        final long t0 = System.nanoTime();

        LOGGER.log(INFO,
                """
                >>> ENTER Combinatorics.getAllPossibleColumnCombinations
                  filterColumns.length = %d
                  filterColumns        = %s
                """.formatted(
                        filterColumns == null ? -1 : filterColumns.length,
                        filterColumns == null ? "null" : Arrays.toString(filterColumns)
                )
        );

        // in case only one condition for join and single filter
        // if(filterColumns.length == 1) return Collections.singletonList(filterColumns);

        if(filterColumns.length == 2) {
            LOGGER.log(INFO, "Combinatorics: taking branch length==2");
            List<int[]> out = getCombinationsFor2SizeColumnList(filterColumns);
            LOGGER.log(INFO,
                    "Combinatorics: branch length==2 DONE, out.size=" + out.size()
                            + " elapsed(ms)=" + ((System.nanoTime() - t0) / 1_000_000.0));
            return out;
        }

        if(filterColumns.length == 3) {
            LOGGER.log(INFO, "Combinatorics: taking branch length==3");
            List<int[]> out = getCombinationsFor3SizeColumnList(filterColumns);
            LOGGER.log(INFO,
                    "Combinatorics: branch length==3 DONE, out.size=" + out.size()
                            + " elapsed(ms)=" + ((System.nanoTime() - t0) / 1_000_000.0));
            return out;
        }

        // note: you had a method for len==4 but you never call it; log that fact
        if(filterColumns.length == 4) {
            LOGGER.log(INFO, "Combinatorics: length==4 but current code DOES NOT use getCombinationsFor4SizeColumnList (will go general case)");
        }

        final int length = filterColumns.length;

        LOGGER.log(INFO, "Combinatorics: entering general case, length=" + length);

        // not exact number, an approximation!
        int totalComb = (int) Math.pow( length, 2 );

        LOGGER.log(INFO, "Combinatorics: totalComb approx=" + totalComb);

        List<int[]> listRef = new ArrayList<>( totalComb );

        LOGGER.log(INFO,
                "Combinatorics: listRef created with initialCapacity=" + totalComb
                        + " elapsed(ms)=" + ((System.nanoTime() - t0) / 1_000_000.0));

        for(int i = 0; i < length - 1; i++){

            // progress marker on outer loop
            LOGGER.log(INFO,
                    "Combinatorics: OUTER LOOP i=" + i
                            + " / " + (length - 2)
                            + " currentSize=" + listRef.size()
                            + " elapsed(ms)=" + ((System.nanoTime() - t0) / 1_000_000.0));

            int[] base = Arrays.copyOfRange( filterColumns, i, i+1 );

            // log base creation
            LOGGER.log(INFO,
                    "  i=" + i
                            + " base=" + Arrays.toString(base)
                            + " add(base)");

            listRef.add ( base );

            for(int j = i+1; j < length; j++ ) {

                // progress marker on inner loop (not every iteration, to avoid spam)
                if ((j - (i+1)) % 25 == 0) {
                    LOGGER.log(INFO,
                            "    INNER LOOP i=" + i + " j=" + j
                                    + " currentSize=" + listRef.size()
                                    + " elapsed(ms)=" + ((System.nanoTime() - t0) / 1_000_000.0));
                }

                int[] range = Arrays.copyOfRange(filterColumns, i, j + 1);
                listRef.add(range);

                // now get all possibilities without this j
                if (j < length - 1) {
                    int k = j + 1;

                    int[] aux1 = {filterColumns[i], filterColumns[k]};
                    listRef.add(aux1);

                    if (k < length - 1) {
                        int[] aux2 = Arrays.copyOfRange(filterColumns, k, length);
                        int[] aux3 = Arrays.copyOf(base, base.length + aux2.length);
                        System.arraycopy(aux2, 0, aux3, base.length, aux2.length);
                        listRef.add(aux3);
                    }
                }
            }
        }

        int[] last = Arrays.copyOfRange( filterColumns, length - 1, length );
        listRef.add(last);

        LOGGER.log(INFO,
                """
                >>> EXIT Combinatorics.getAllPossibleColumnCombinations
                  finalSize   = %d
                  lastAdded   = %s
                  elapsed(ms) = %.3f
                """.formatted(
                        listRef.size(),
                        Arrays.toString(last),
                        (System.nanoTime() - t0) / 1_000_000.0
                )
        );

        return listRef;
    }
}