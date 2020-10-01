/*****************************************************************************
 * Given raw data, create four INDEXes that can be used to perform
 * fuzzy matches on the data:
 *
 *      hashValue -> entityID
 *      entityID -> hashValue
 *
 *      matchID -> entityID
 *      entityID -> matchID
 *****************************************************************************/

IMPORT $.^.Medley;

#WORKUNIT('name', $.Job.WU_PREFIX + ': Create Lookup');
#OPTION('pickBestEngine', FALSE);

//-----------------------------------------------------------------------------

Medley.BuildAllIndexes
    (
        $.Job.SourceData.File,
        #EXPAND($.Job.SourceData.EntityIDFieldName),
        $.Job.FRAG_DIRECTIVE,
        maxEditDistance := $.Job.FRAG_EDIT_DISTANCE,
        id2HashIndexPath := $.Job.ID2HASH_LOOKUP_PATH,
        hash2IDIndexPath := $.Job.HASH2ID_LOOKUP_PATH,
        id2MatchIndexPath := $.Job.ID2MATCH_LOOKUP_PATH,
        match2IDIndexPath := $.Job.MATCH2ID_LOOKUP_PATH
    );
