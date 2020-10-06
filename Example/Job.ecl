/**
 * Configuration information used by the BWRs in this directory.  Specific
 * items should be modified:
 *
 *      JOB_NICKNAME                    A string that is inserted into both
 •                                      the logical pathnames of created
 •                                      files and the workunit; ideally,
 •                                      a single string that provides a
 •                                      hint on what the job is about
 *      FILESYSTEM_PREFIX               A logical pathname scope (the prefix);
 •                                      all creates files will be placed
 •                                      within this scope
 *      SourceData.File                 This should reference a dataset
 •                                      that is the source of the data you
 •                                      want to index; can be either a
 •                                      reference or a function that
 •                                      returns a DATASET
 *      SourceData.EntityIDFieldName    A string naming the unique
 •                                      identifier field within
 •                                      SourceData.File
 *      FRAG_DIRECTIVE                  The fragmentation directive to use
 •                                      when creating lookup indexes
 •                                      (see Medley.CreateLookupTable())
 •                                      and when searching for related IDs
 •                                      (see Medley.FindRelatedIDsFromLookupTable());
 •                                      see the file comments within the Medley
 *                                      module for further details on the
 •                                      format for this string
 *      FRAG_EDIT_DISTANCE              The maximum edit distance for field
 •                                      group-level deletion neighborhood
 •                                      generation;
 •                                      see Medley.CreateLookupTable()
 •                                      and Medley.FindRelatedIDsFromLookupTable()
 */
EXPORT Job := MODULE

    SHARED JOB_NICKNAME := 'demo';

    EXPORT FILESYSTEM_PREFIX := '~medley::sample';

    SHARED FILESYSTEM_EXTENDED_PREFIX := FILESYSTEM_PREFIX + IF(JOB_NICKNAME != '', '::' + JOB_NICKNAME, '');

    EXPORT FILESYSTEM_CACHE_PREFIX := FILESYSTEM_EXTENDED_PREFIX + '::cache';

    EXPORT HASH2ID_LOOKUP_PATH := FILESYSTEM_EXTENDED_PREFIX + '::hash2id_lookup_key';
    EXPORT ID2HASH_LOOKUP_PATH := FILESYSTEM_EXTENDED_PREFIX + '::id2hash_lookup_key';

    EXPORT MATCH2ID_LOOKUP_PATH := FILESYSTEM_EXTENDED_PREFIX + '::match2id_lookup_key';
    EXPORT ID2MATCH_LOOKUP_PATH := FILESYSTEM_EXTENDED_PREFIX + '::id2match_lookup_key';

    EXPORT WU_PREFIX := 'Medley' + IF(JOB_NICKNAME != '', ' (' + JOB_NICKNAME + ')', '');

    EXPORT SourceData := MODULE

        EXPORT EntityIDFieldName := 'guid';

        EXPORT RawDataLayout := RECORDOF($.SampleData);

        EXPORT Layout := RECORD
            RawDataLayout;
            STRING      street1_only;
            UNSIGNED4   bdate_yyyy;
        END;

        EXPORT Layout MakeNewRec(RawDataLayout oldRec) := TRANSFORM
            SELF.street1_only := REGEXFIND('\\d+ (.+)', oldRec.address1, 1);
            SELF.bdate_yyyy := oldRec.bdate DIV 10000;
            SELF := oldRec;
        END;

        EXPORT File := PROJECT(DISTRIBUTE($.SampleData, SKEW(0.05)), MakeNewRec(LEFT));

    END; // SourceData module

    EXPORT FRAG_DIRECTIVE :=
        [
            'fname%1,lname%1;bdate_yyyy;street1_only,city,state;age',   // first directive
            'lname,city,state,age'                                      // second directive
        ];
    EXPORT FRAG_EDIT_DISTANCE := 1;

END; // Job module
