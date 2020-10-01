/*****************************************************************************
 * Given string entries for fields that have been previously indexed, perform
 * a search for related entities and return only their entity IDs.
 * The result will in the format of Medley.(IDLayout):
 *
 *      RECORD
 *          UNSIGNED6   id;         // A related entity ID
 *      END;
 *
 * This will run efficiently in hthor or ROXIE.
 *****************************************************************************/

IMPORT $.^.Medley;

#WORKUNIT('name', $.Job.WU_PREFIX + ': Search For Related Entity IDs');

//-----------------------------------------------------------------------------

// Set up this way to make it easy to convert to a ROXIE query

STRING p_fname      := 'FREDDIE' : STORED('fname');
STRING p_mname      := '' : STORED('mname');
STRING p_lname      := 'BAIRD' : STORED('lname');
STRING p_sex        := '' : STORED('sex');
STRING p_bdate      := '19640320' : STORED('bdate');
STRING p_address1   := '639 Maple Street' : STORED('address1');
STRING p_address2   := '' : STORED('address2');
STRING p_city       := 'Greenwood' : STORED('city');
STRING p_state      := 'NE' : STORED('state');
STRING p_income     := '115831' : STORED('income');
STRING p_age        := '75' : STORED('age');

//-----------------

rawSearchData := DATASET
    (
        [
            {0, p_fname, p_mname, p_lname, p_sex, p_bdate, p_address1, p_address2, p_city, p_state, p_income, p_age}
        ],
        $.Job.SourceData.RawDataLayout
    );

searchData := PROJECT(rawSearchData, $.Job.SourceData.MakeNewRec(LEFT));

//-----------------

lookupTable := Medley.CreateLookupTable
    (
        searchData,
        #EXPAND($.Job.SourceData.EntityIDFieldName),
        $.Job.FRAG_DIRECTIVE,
        $.Job.FRAG_EDIT_DISTANCE
    );

relatedIDs := Medley.FindRelatedIDsFromLookupTable
    (
        lookupTable,
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
        TRANSFORM(LEFT)
    );

final := SORT(relatedIDsWithData, #EXPAND($.Job.SourceData.EntityIDFieldName));

ORDERED
    (
        OUTPUT(relatedIDs, NAMED('found_ids'), ALL);
        OUTPUT(final, NAMED('related_data'), ALL);
    );
