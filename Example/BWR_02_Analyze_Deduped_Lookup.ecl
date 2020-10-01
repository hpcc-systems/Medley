/*****************************************************************************
 * Perform some duplication analysis using the ID<->MatchingID index built
 * with BWR_01_Create_Lookup_Indexes.ecl.
 *****************************************************************************/

IMPORT $.^.Medley;

#WORKUNIT('name', $.Job.WU_PREFIX + ': Analyze Deduped Data');
#OPTION('pickBestEngine', FALSE);

//-----------------------------------------------------------------------------

DEDUPED_FILE_PATH := $.Job.FILESYSTEM_PREFIX + '::deduped';

//-----------------------------------------------------------------------------

// Create a dedup file from the indexes created in BWR_01_* joined with our
// original data

matchingIDX := Medley.ID2MatchLookupIndexDef($.Job.ID2MATCH_LOOKUP_PATH);

expandedWithData0 := JOIN
    (
        $.Job.SourceData.File,
        matchingIDX,
        LEFT.#EXPAND($.Job.SourceData.EntityIDFieldName) = RIGHT.id,
        TRANSFORM
            (
                {
                    Medley.CollapsedMatchingLayout.matching_id,
                    RECORDOF(LEFT)
                },
                SELF.matching_id := RIGHT.matching_id,
                SELF := LEFT
            )
    );

expandedWithData := SORT(expandedWithData0, matching_id, #EXPAND($.Job.SourceData.EntityIDFieldName)) : PERSIST($.Job.FILESYSTEM_CACHE_PREFIX + '::dedup_analysis', SINGLE);

//-----------------------------------------------------------------------------

stat0 := TABLE
    (
        expandedWithData,
        {
            matching_id,
            UNSIGNED4       cnt := COUNT(GROUP)
        },
        matching_id,
        MERGE
    );

OUTPUT(COUNT(stat0), NAMED('match_count'));
OUTPUT(MAX(stat0, cnt), NAMED('most_entity_matches'));
OUTPUT(ROUND(AVE(stat0, cnt)), NAMED('ave_entity_matches'));

stat1 := TABLE
    (
        stat0,
        {
            UNSIGNED4   cluster_size := cnt,
            UNSIGNED4   num_entities := COUNT(GROUP)
        },
        cnt
    );

OUTPUT(SORT(stat1, -num_entities, -cluster_size), NAMED('cluster_size_counts'), ALL);

ShowSamples(UNSIGNED4 numMatches, STRING outName = '') := FUNCTION
    SAMPLE_SIZE := 500;
    stat := stat0(cnt = numMatches);
    fileData := JOIN
        (
            stat,
            expandedWithData,
            LEFT.matching_id = RIGHT.matching_id,
            TRANSFORM(RIGHT)
        );
    myName := IF(outName != '', outName, (STRING)numMatches);
    // Make sure we output all records for a match group
    chooseSize := IF(SAMPLE_SIZE % numMatches != 0, ((SAMPLE_SIZE DIV numMatches) + 1) * numMatches, SAMPLE_SIZE);
    RETURN PARALLEL
        (
            OUTPUT(CHOOSEN(fileData, chooseSize), NAMED('entity_match_' + myName + '_sample'));
            OUTPUT(COUNT(fileData), NAMED('entity_match_' + myName + '_rec_cnt'));
        );
END;

ShowSamples(2);
ShowSamples(3);
ShowSamples(4);
ShowSamples(5);
ShowSamples(6);
ShowSamples(7);
ShowSamples(8);
ShowSamples(9);
ShowSamples(MAX(stat0, cnt), 'max');
