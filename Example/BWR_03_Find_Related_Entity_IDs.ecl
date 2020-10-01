/*****************************************************************************
 * Given a SET of entity IDs, find related entity IDs.  The result will in the
 * format of Medley.(RelatedIDLayout):
 *
 *      RECORD
 *          UNSIGNED6   given_id;   // One of the entity IDs from the given SET
 *          UNSIGNED6   id;         // A related entity ID
 *      END;
 *
 * This will run efficiently in hthor or ROXIE.
 *****************************************************************************/

IMPORT $.^.Medley;
IMPORT Std;

#WORKUNIT('name', $.Job.WU_PREFIX + ': Find Related Entity IDs');

//-----------------------------------------------------------------------------

GIVEN_ENTITY_IDS := [7045, 9465];

//------------------------------------

relatedIDs := Medley.FindRelatedIDs
    (
        DATASET(GIVEN_ENTITY_IDS, Medley.IDLayout),
        $.Job.ID2HASH_LOOKUP_PATH,
        $.Job.HASH2ID_LOOKUP_PATH,
        $.Job.ID2MATCH_LOOKUP_PATH,
        $.Job.MATCH2ID_LOOKUP_PATH
    );

// Append our original data so we can eyeball the similarities
relatedIDsWithData := JOIN
    (
        $.Job.SourceData.File,
        relatedIDs,
        LEFT.#EXPAND($.Job.SourceData.EntityIDFieldName) = RIGHT.id,
        TRANSFORM
            (
                {
                    Medley.ID_t     given_id,
                    RECORDOF(LEFT)
                },
                SELF.given_id := RIGHT.given_id,
                SELF := LEFT
            )
    );

final := SORT(relatedIDsWithData, given_id, #EXPAND($.Job.SourceData.EntityIDFieldName));

ORDERED
    (
        OUTPUT(relatedIDs, NAMED('related_ids'), ALL);
        OUTPUT(final, NAMED('related_data'), ALL);
    );
