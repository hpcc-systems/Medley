/**
 * Code for performing record fragmentation matching.  The module provides
 * the following functionality:
 *
 *      - Creation of search indexes, linked back to entity ID values
 *      - Fuzzy matching within the source dataset, linking similar IDs
 *      - Searching for related IDs, given a dataset of IDs
 *      - Searching for related IDs, given a dataset of data mimicking the
 *        source dataset
 *
 * LexisNexis Risk Solutions patent pending as of June 30, 2020.
 *
 * ----------------------------------------------------------------------------
 * EXPORTED ATTRIBUTES
 * ----------------------------------------------------------------------------
 *
 *      DATA TYPES
 *          ID_t := UNSIGNED6;
 *          Hash_t := UNSIGNED8;
 *          MatchingID_t := UNSIGNED4;
 *
 *      RECORD DEFINITIONS
 *          LookupTableLayout
 *          CollapsedMatchingLayout
 *          IDLayout
 *          RelatedIDLayout
 *
 *      INDEX FILE DEFINITIONS
 *          Hash2IDLookupIndexDef(...) := INDEX
 *          ID2HashLookupIndexDef(...) := INDEX
 *          Match2IDLookupIndexDef(...) := INDEX
 *          ID2MatchLookupIndexDef(...) := INDEX
 *
 *      FUNCTIONS (see actual functions for detailed documentation)
 *          CreateLookupTable(...) := FUNCTIONMACRO
 *          CollapseLookupTable(...) := FUNCTION
 *          WriteIDLookupIndexes(...) := FUNCTION
 *          WriteMatchIDIndexes(...) := FUNCTION
 *          BuildAllIndexes(...) := FUNCTIONMACRO
 *          FindRelatedIDs(...) := FUNCTION
 *          FindRelatedIDsFromLookupTable(...) := FUNCTION
 */
EXPORT Medley := MODULE

    //-------------------------------------------------------------------------
    // Version information
    //-------------------------------------------------------------------------
    EXPORT UNSIGNED1 VERSION_MAJOR := 0;
    EXPORT UNSIGNED1 VERSION_MINOR := 6;
    EXPORT UNSIGNED1 VERSION_POINT := 1;
    EXPORT STRING VERSION_STRING := VERSION_MAJOR + '.' + VERSION_MINOR + '.' + VERSION_POINT;

    //-------------------------------------------------------------------------
    // Data types
    //-------------------------------------------------------------------------
    EXPORT ID_t := UNSIGNED6;
    EXPORT Hash_t := UNSIGNED8;
    EXPORT MatchingID_t := UNSIGNED4;

    //-------------------------------------------------------------------------
    // Exported record definitions
    //-------------------------------------------------------------------------
    EXPORT LookupTableLayout := RECORD
        ID_t                id;
        Hash_t              hash_value;
    END;

    EXPORT CollapsedMatchingLayout := RECORD
        MatchingID_t        matching_id;
        ID_t                id;
    END;

    EXPORT IDLayout := RECORD
        ID_t                id;
    END;

    EXPORT RelatedIDLayout := RECORD
        ID_t                given_id;
        ID_t                id;
    END;

    //-------------------------------------------------------------------------
    // Exported index declarations
    //-------------------------------------------------------------------------
    EXPORT Hash2IDLookupIndexDef(STRING path) := INDEX
        (
            {LookupTableLayout.hash_value},
            {LookupTableLayout},
            path
        );

    EXPORT ID2HashLookupIndexDef(STRING path) := INDEX
        (
            {LookupTableLayout.id},
            {LookupTableLayout},
            path
        );

    EXPORT Match2IDLookupIndexDef(STRING path) := INDEX
        (
            {CollapsedMatchingLayout.matching_id},
            {CollapsedMatchingLayout},
            path
        );

    EXPORT ID2MatchLookupIndexDef(STRING path) := INDEX
        (
            {CollapsedMatchingLayout.id},
            {CollapsedMatchingLayout},
            path
        );

    //-------------------------------------------------------------------------
    // Protected record definitions
    //-------------------------------------------------------------------------
    SHARED MatchIDPairsRec := RECORD
        MatchingID_t    matching_id;
        MatchingID_t    related_matching_id;
    END;

    /**
     * Embedded function for reducing linked MatchingID_t pairs.  Note that
     * reduction occurs on only a per-worker basis; it is possible that
     * further reductions are possible in a global process.
     *
     * The following must be true within the argument:
     *
     *      SORT(SORT(ds, related_matching_id), matching_id, related_matching_id, LOCAL)
     *      matching_id < related_matching_id for all records
     *
     * @param   ds      A DATASET(MatchIDPairsRec) to process
     *
     * @return  A new dataset in the same layout as the argument, with
     *          all matching_id values reduced as far as possible.
     */
    SHARED STREAMED DATASET(MatchIDPairsRec) LocallyReduceMatchPairs(STREAMED DATASET(MatchIDPairsRec) ds) := EMBED(C++ : activity)
        #include <map>

        typedef uint32_t ID_ELEMENT_TYPE;
        typedef std::map<ID_ELEMENT_TYPE, ID_ELEMENT_TYPE> MatchMap;

        class MatchIDPairDataset : public RtlCInterface, implements IRowStream
        {
            public:

                MatchIDPairDataset(IEngineRowAllocator* _resultAllocator, IRowStream* _ds)
                    :   resultAllocator(_resultAllocator), matchDataDS(_ds)
                {}

                RTLIMPLEMENT_IINTERFACE

                virtual const void* nextRow() override
                {
                    const byte*     oneRow = static_cast<const byte*>(matchDataDS->nextRow());

                    if (!oneRow)
                        return nullptr;

                    ID_ELEMENT_TYPE     matchingID = *((ID_ELEMENT_TYPE*)oneRow);
                    ID_ELEMENT_TYPE     relatedMatchingID = *((ID_ELEMENT_TYPE*)(oneRow + sizeof(ID_ELEMENT_TYPE)));
                    MatchMap::iterator  matchingIter = theMap.find(matchingID);

                    if (matchingIter != theMap.end())
                    {
                        matchingID = matchingIter->second;
                    }

                    theMap[relatedMatchingID] = matchingID;

                    RtlDynamicRowBuilder    rowBuilder(resultAllocator);
                    byte*                   newRow = rowBuilder.getSelf();

                    *((ID_ELEMENT_TYPE*)newRow) = matchingID;
                    *((ID_ELEMENT_TYPE*)(newRow + sizeof(ID_ELEMENT_TYPE))) = relatedMatchingID;

                    // Delete entries we no longer need
                    for (MatchMap::iterator it = theMap.begin(); it != theMap.end() && it->first < matchingID;)
                    {
                        it = theMap.erase(it);
                    }

                    return rowBuilder.finalizeRowClear(sizeof(ID_ELEMENT_TYPE) + sizeof(ID_ELEMENT_TYPE));
                }

                virtual void stop() override
                {
                    matchDataDS->stop();
                }

            protected:

                Linked<IEngineRowAllocator> resultAllocator;
                IRowStream*                 matchDataDS;
                MatchMap                    theMap;
        };

        #body

        return new MatchIDPairDataset(_resultAllocator, ds);
    ENDEMBED;

    /**
     * Function macro that creates deletion neighborhoods for an entire dataset.
     * Both inter-field and intra-field deletion neighborhoods may be created,
     * depending on the value of the fieldSpec parameter.
     *
     * @param   inFile              The dataset to process; REQUIRED
     * @param   idField             The unique identifier field within each
     *                              record; this is not a string; the field's
     *                              data type should match the ID_t definition
     *                              above; REQUIRED
     * @param   fieldSpec           A description of how the dataset should be
     *                              processed; can be a single STRING or a
     *                              SET OF STRING to define several descriptions
     *                              that are combined via OR; within each STRING,
     *                              semi-colons are used to delimit field groups,
     *                              commas are used to delimit fields; field
     *                              names may optionally have a suffix of '%N'
     *                              where N is the maximum edit distance to use
     *                              when creating an inter-field deletion
     *                              neighborhood; fields and field groups may
     *                              have a '&' prefix to indicate that the field
     *                              or field group should not be deleted when
     *                              constructing the intra-field deletion
     *                              neighhborhood; REQUIRED
     * @param   maxEditDistance     The maximum intra-field edit distance to
     *                              create; OPTIONAL, defaults to 1
     *
     * @return  A new DATASET(LookupTableLayout) dataset.
     */
    EXPORT CreateLookupTable(inFile, idField, fieldSpec, maxEditDistance = 1) := FUNCTIONMACRO
        // Embedded function for creating substrings mapping to a deletion neighborhood;
        // note that the strings themselves are returned, not a hash of the string
        #UNIQUENAME(CreateStringDeletionNeighborhood);
        STREAMED DATASET({UTF8 text}) %CreateStringDeletionNeighborhood%(CONST UTF8 text, UNSIGNED1 max_edit_distance) := EMBED(C++)
            #option pure;
            #include <set>
            #include <string>

            #define UCHAR_TYPE uint16_t
            #include <unicode/unistr.h>

            typedef std::set<std::string> TextSet;

            using icu::UnicodeString;

            class StreamedStringDataset : public RtlCInterface, implements IRowStream
            {
                public:

                    StreamedStringDataset(IEngineRowAllocator* _resultAllocator, unsigned int _word_byte_count, const char* _word, unsigned int _max_edit_distance)
                        : resultAllocator(_resultAllocator), myText(_word, _word_byte_count, "UTF-8"), myEditDistance(_max_edit_distance), isInited(false)
                    {
                        isStopped = false;
                    }

                    RTLIMPLEMENT_IINTERFACE

                    void AppendToCollection(const UnicodeString& textLine)
                    {
                        outString.clear();
                        textLine.toUTF8String(outString);
                        deletionNeighborhood.insert(outString);
                    }

                    void PopulateDeletionNeighborhood(const UnicodeString& textLine, unsigned int depth)
                    {
                        if (depth > 0 && textLine.countChar32() > 2)
                        {
                            UnicodeString   myTextLine;

                            for (int32_t x = 0; x < textLine.countChar32(); x++)
                            {
                                myTextLine = textLine;
                                myTextLine.remove(x, 1);
                                AppendToCollection(myTextLine);
                                PopulateDeletionNeighborhood(myTextLine, depth - 1);
                            }
                        }
                    }

                    virtual const void* nextRow()
                    {
                        if (isStopped)
                        {
                            return NULL;
                        }

                        if (!isInited)
                        {
                            AppendToCollection(myText);
                            PopulateDeletionNeighborhood(myText, myEditDistance);
                            deletionNeighborhoodIter = deletionNeighborhood.begin();
                            isInited = true;
                        }

                        if (deletionNeighborhoodIter != deletionNeighborhood.end())
                        {
                            const std::string&      textLine = *deletionNeighborhoodIter;
                            RtlDynamicRowBuilder    rowBuilder(resultAllocator);
                            unsigned int            len = sizeof(__int32) + textLine.size();
                            byte*                   row = rowBuilder.ensureCapacity(len, NULL);

                            *(__int32*)(row) = rtlUtf8Length(textLine.size(), textLine.data());
                            memcpy(row + sizeof(__int32), textLine.data(), textLine.size());

                            ++deletionNeighborhoodIter;

                            return rowBuilder.finalizeRowClear(len);
                        }

                        isStopped = true;

                        return NULL;
                    }

                    virtual void stop()
                    {
                        isStopped = true;
                    }

                protected:

                    Linked<IEngineRowAllocator> resultAllocator;

                private:

                    UnicodeString               myText;
                    unsigned int                myEditDistance;
                    TextSet                     deletionNeighborhood;
                    TextSet::const_iterator     deletionNeighborhoodIter;
                    std::string                 outString;
                    bool                        isInited;
                    bool                        isStopped;
            };

            #body

            return new StreamedStringDataset(_resultAllocator, rtlUtf8Size(lenText, text), text, max_edit_distance);
        ENDEMBED;

        // Embedded function for creating a numeric deletion neighborhood from a set
        // of numbers;
        #UNIQUENAME(CreateNumericSetDeletionNeighborhood);
        STREAMED DATASET({#$.Medley.Hash_t hash_value}) %CreateNumericSetDeletionNeighborhood%(SET OF #$.Medley.Hash_t _attr_set, UNSIGNED1 max_edit_distance) := EMBED(C++)
            #option pure;
            #include <set>
            #include <vector>

            typedef std::vector<hash64_t> AttrList;
            typedef std::set<hash64_t> HashSet;

            class StreamedHashValueDataset : public RtlCInterface, implements IRowStream
            {
                public:

                    StreamedHashValueDataset(IEngineRowAllocator* _resultAllocator, const hash64_t* _set_values, unsigned _num_values, unsigned int _max_edit_distance)
                        : resultAllocator(_resultAllocator), myEditDistance(_max_edit_distance), isInited(false)
                    {
                        isStopped = (_set_values != nullptr && _num_values == 0);

                        if (!isStopped)
                        {
                            for (unsigned int x = 0; x < _num_values; x++)
                            {
                                setValues.push_back(_set_values[x]);
                            }
                        }
                    }

                    RTLIMPLEMENT_IINTERFACE

                    hash64_t HashStdList(AttrList& myAttrList)
                    {
                        hash64_t    hashValue = HASH64_INIT;

                        for (AttrList::const_iterator x = myAttrList.begin(); x != myAttrList.end(); x++)
                        {
                            hash64_t    element = *x;

                            hashValue = rtlHash64Data(sizeof(element), &element, hashValue);
                        }

                        return hashValue;
                    }

                    void PopulateDeletionNeighborhood(AttrList theAttrList, unsigned int depth)
                    {
                        if (depth > 0 && theAttrList.size() > 1)
                        {
                            AttrList    myAttrList(theAttrList.size() - 1, 0); // reserve space

                            for (unsigned int x = 0; x < theAttrList.size(); x++)
                            {
                                if (x > 0)
                                {
                                    // Copy over the next single element that will not change in
                                    // subsequent loop iterations
                                    myAttrList[x - 1] = theAttrList[x - 1];
                                }

                                unsigned int    insertPos = x;

                                // Copy remaining elements
                                for (unsigned int y = x + 1; y < theAttrList.size(); y++)
                                {
                                    myAttrList[insertPos++] = theAttrList[y];
                                }

                                deletionNeighborhood.insert(HashStdList(myAttrList));
                                PopulateDeletionNeighborhood(myAttrList, depth - 1);
                            }
                        }
                    }

                    virtual const void* nextRow()
                    {
                        if (isStopped)
                        {
                            return NULL;
                        }

                        if (!isInited)
                        {
                            deletionNeighborhood.insert(HashStdList(setValues));

                            PopulateDeletionNeighborhood(setValues, myEditDistance);

                            deletionNeighborhoodIter = deletionNeighborhood.begin();
                            isInited = true;
                        }

                        if (deletionNeighborhoodIter != deletionNeighborhood.end())
                        {
                            hash64_t                oneHash = *deletionNeighborhoodIter;
                            RtlDynamicRowBuilder    rowBuilder(resultAllocator);
                            unsigned int            len = sizeof(oneHash);
                            byte*                   row = rowBuilder.ensureCapacity(len, NULL);

                            *(hash64_t*)(row) = oneHash;

                            ++deletionNeighborhoodIter;

                            return rowBuilder.finalizeRowClear(len);
                        }

                        isStopped = true;

                        return NULL;
                    }

                    virtual void stop()
                    {
                        isStopped = true;
                    }

                protected:

                    Linked<IEngineRowAllocator> resultAllocator;

                private:

                    AttrList                    setValues;
                    unsigned int                myEditDistance;
                    HashSet                     deletionNeighborhood;
                    HashSet::const_iterator     deletionNeighborhoodIter;
                    bool                        isInited;
                    bool                        isStopped;
            };

            #body

            hash64_t*   setSource = static_cast<hash64_t*>(const_cast<void*>(_attr_set));
            unsigned    numElements = len_attr_set / sizeof(hash64_t);

            return new StreamedHashValueDataset(_resultAllocator, setSource, numElements, (max_edit_distance > numElements - 1 ? numElements - 1 : max_edit_distance));
        ENDEMBED;

        // Housekeeping involving fieldSpec, which could be a single spec (string)
        // or multiple specs (set of strings)
        #UNIQUENAME(fieldSpecType);
        #SET(fieldSpecType, #GETDATATYPE(fieldSpec));
        #UNIQUENAME(fieldSpecIsSet);
        #UNIQUENAME(numFieldSpecs);
        #IF(%'fieldSpecType'%[..7] = 'set of ')
            #SET(fieldSpecIsSet, 1)
            #SET(numFieldSpecs, COUNT(fieldSpec))
        #ELSE
            #SET(fieldSpecIsSet, 0)
            #SET(numFieldSpecs, 1)
        #END
        #UNIQUENAME(fieldSpecIter)
        #SET(fieldSpecIter, 1);

        // Distribute the incoming data on the ID field, so we can localize operations later
        #UNIQUENAME(distInFile);
        LOCAL %distInFile% := DISTRIBUTE(inFile, HASH64((#$.Medley.ID_t)idField));

        // Make sure edit distance is non-negative
        #UNIQUENAME(myMaxEditDistance);
        #SET(myMaxEditDistance, (UNSIGNED1)MAX((INTEGER1)maxEditDistance, 0));

        // Placeholder for some built-up ECL, combining the results from
        // multiple field specs
        #UNIQUENAME(combineLookupTableStmt);
        #SET(combineLookupTableStmt, '');

        #LOOP
            #UNIQUENAME(interimLookupTable);
            #UNIQUENAME(myFieldSpec)
            #UNIQUENAME(requiredFieldSpec)
            #UNIQUENAME(requiredFieldSpecCount)
            #UNIQUENAME(otherFieldSpec)
            #UNIQUENAME(fieldList)
            #UNIQUENAME(neighborhoodFields)
            #UNIQUENAME(temp)
            #UNIQUENAME(tempName)
            #UNIQUENAME(tempVal)
            #UNIQUENAME(tempSaved)
            #UNIQUENAME(pos)
            #UNIQUENAME(needDelim)

            #IF(%fieldSpecIter% <= %numFieldSpecs%)
                // Remove spaces from fieldSpec, insert default edit distance where needed
                #IF(%fieldSpecIsSet% = 0)
                    #SET(myFieldSpec, REGEXREPLACE('%([^\\d])', TRIM((STRING)fieldSpec, ALL), '%1$1'))
                #ELSE
                    #SET(myFieldSpec, REGEXREPLACE('%([^\\d])', TRIM((STRING)(fieldSpec[%fieldSpecIter%]), ALL), '%1$1'))
                #END

                // Split field spec into required and other
                #SET(requiredFieldSpec, '')
                #SET(requiredFieldSpecCount, 0)
                #SET(otherFieldSpec, '')
                #SET(pos, 1)
                #LOOP
                    #SET(temp, REGEXFIND('([^;]+)', %'myFieldSpec'%[%pos%..], 1))
                    #IF(%'temp'% != '')
                        #IF(%'temp'%[1] = '&')
                            #IF(%requiredFieldSpecCount% > 0)
                                #APPEND(requiredFieldSpec, ';')
                            #END
                            #APPEND(requiredFieldSpec, %'temp'%[2..])
                            #SET(requiredFieldSpecCount, %requiredFieldSpecCount% + 1);
                        #ELSE
                            #IF(%'otherFieldSpec'% != '')
                                #APPEND(otherFieldSpec, ';')
                            #END
                            #APPEND(otherFieldSpec, %'temp'%)
                        #END
                        #SET(pos, %pos% + LENGTH(%'temp'%) + 1)
                    #ELSE
                        #BREAK
                    #END
                #END

                // Check for embedded required field group indicators (there should be none)
                #IF(REGEXFIND('&', %'requiredFieldSpec'% + %'otherFieldSpec'%))
                    #ERROR('"' + fieldSpec + '" contains a required indicator (&) on an individual field within a field group')
                #END

                // Remove any required character patterns from the full field spec
                #SET(myFieldSpec, REGEXREPLACE('&', %'myFieldSpec'%, ''))

                // Find all fields where we need to create deletion neighborhoods on their values,
                // and the maximum edit distance cited (in case the field was defined that way
                // more than once)
                #SET(neighborhoodFields, ',')
                #SET(pos, 1)
                #LOOP
                    #SET(temp, REGEXFIND('([^,;%]+%\\d+)', %'myFieldSpec'%[%pos%..], 1))
                    #SET(tempName, REGEXFIND('^([^%]+)%', %'temp'%, 1))
                    #SET(tempVal, REGEXFIND('%(\\d+)', %'temp'%, 1))
                    #IF(%'tempName'% != '' AND %'tempVal'% != '')
                        #SET(tempSaved, REGEXFIND('(,' + %'tempName'% + '%\\d+)', %'neighborhoodFields'%, 1))
                        #IF(%'tempSaved'% != '')
                            #SET(neighborhoodFields, REGEXREPLACE(',' + %'tempSaved'%, %'neighborhoodFields'%, ',' + %'tempName'% + '%' + (STRING)(MAX((UNSIGNED2)REGEXFIND('%(\\d+)', %'tempSaved'%, 1), (UNSIGNED2)%'tempVal'%))))
                        #ELSE
                            #APPEND(neighborhoodFields, %'temp'% + ',')
                        #END
                        #SET(pos, %pos% + LENGTH(REGEXFIND('^.+?[^,;%]+%\\d+', %'myFieldSpec'%[%pos%..], 0)) + 1)
                    #ELSE
                        #BREAK
                    #END
                #END
                #SET(neighborhoodFields, REGEXREPLACE('^,', %'neighborhoodFields'%, ''))
                #SET(neighborhoodFields, REGEXREPLACE(',$', %'neighborhoodFields'%, ''))

                // Record structure containing only the fields we want, coerced into UTF8 strings
                #UNIQUENAME(FileRec)
                LOCAL %FileRec% := RECORD
                    #$.Medley.ID_t  id;

                    #SET(fieldList, '')
                    #SET(pos, 1)
                    #LOOP
                        #SET(temp, REGEXFIND('^([^,;%]+)', %'myFieldSpec'%[%pos%..], 1))
                        #IF(%'temp'% != '')
                            #SET(tempSaved, REGEXFIND('^' + %'temp'% + '(%\\d+)?[,;]*', %'myFieldSpec'%[%pos%..], 0))
                            #IF(NOT REGEXFIND('\\b' + %'temp'% + '\\b', %'fieldList'%))
                                #IF(%'fieldList'% != '')
                                    #APPEND(fieldList, ',')
                                #END
                                #APPEND(fieldList, %'temp'%)
                                // Include the field in the record definition
                                UTF8    %temp%;
                            #END
                            #SET(pos, %pos% + LENGTH(%'tempSaved'%))
                        #ELSE
                            #BREAK
                        #END
                    #END
                END;

                // Create working dataset
                #UNIQUENAME(workingFile)
                LOCAL %workingFile% := PROJECT
                    (
                        %distInFile%,
                        TRANSFORM
                            (
                                %FileRec%,
                                SELF.id := (#$.Medley.ID_t)LEFT.idField
                                #SET(pos, 1)
                                #LOOP
                                    #SET(temp, REGEXFIND('^([^,]+)', %'fieldList'%[%pos%..], 1))
                                    #IF(%'temp'% != '')
                                        , SELF.%temp% := (UTF8)LEFT.%temp%
                                        #SET(pos, %pos% + LENGTH(%'temp'%) + 1)
                                    #ELSE
                                        #BREAK
                                    #END
                                #END
                            )
                    );

                // Expand field values with deletion neighborhood entries, if any
                #UNIQUENAME(expandedWorkingFile)
                #IF(%'neighborhoodFields'% != '')
                    #UNIQUENAME(resultNameBase)
                    #UNIQUENAME(resultNameCounter)
                    #SET(resultNameCounter, 0)
                    #UNIQUENAME(resultName)
                    #SET(resultName, %'resultNameBase'% + %'resultNameCounter'%)
                    #UNIQUENAME(nextResultName)
                    LOCAL %resultName% := %workingFile%;

                    #SET(pos, 1)
                    #LOOP
                        #SET(temp, REGEXFIND('^([^,]+)', %'neighborhoodFields'%[%pos%..], 1))
                        #IF(%'temp'% != '')
                            #SET(tempName, REGEXFIND('^([^%]+)', %'temp'%, 1))
                            #SET(tempVal, REGEXFIND('%(\\d+)', %'temp'%, 1))
                            #SET(resultNameCounter, %resultNameCounter% + 1)
                            #SET(nextResultName, %'resultNameBase'% + %'resultNameCounter'%)

                            LOCAL %nextResultName% := NORMALIZE
                                (
                                    %resultName%,
                                    %CreateStringDeletionNeighborhood%(LEFT.%tempName%, %tempVal%),
                                    TRANSFORM
                                        (
                                            RECORDOF(LEFT),
                                            SELF.%tempName% := RIGHT.text,
                                            SELF := LEFT
                                        )
                                );

                            #SET(resultName, %'nextResultName'%)
                            #SET(pos, %pos% + LENGTH(%'temp'%) + 1)
                        #ELSE
                            #BREAK
                        #END
                    #END

                    LOCAL %expandedWorkingFile% := %resultName%;
                #ELSE
                    LOCAL %expandedWorkingFile% := %workingFile%;
                #END

                // Collect all hashes needed for intra-field deletion neighborhood
                #UNIQUENAME(requiredHashCmd)
                #UNIQUENAME(groupPos)
                #UNIQUENAME(groupFields)
                #UNIQUENAME(bareFieldNames)
                #UNIQUENAME(needsOuterDelim)
                #UNIQUENAME(hashSets)
                LOCAL %hashSets% := PROJECT
                    (
                        %expandedWorkingFile%,
                        TRANSFORM
                            (
                                {
                                    #$.Medley.ID_t          id,
                                    #$.Medley.Hash_t        required_hash_value,
                                    SET OF #$.Medley.Hash_t hash_values
                                },

                                #IF(%requiredFieldSpecCount% > 0)
                                    requiredSet := SET(DATASET
                                        (
                                            [
                                                #SET(groupPos, 1)
                                                #SET(needsOuterDelim, 0)
                                                #SET(bareFieldNames, REGEXREPLACE('%\\d+', %'requiredFieldSpec'%, ''))
                                                #LOOP
                                                    #SET(groupFields, REGEXFIND('^([^;]+)', %'bareFieldNames'%[%groupPos%..], 1))
                                                    #IF(%'groupFields'% != '')
                                                        #IF(%needsOuterDelim% = 1) , #END
                                                        HASH64
                                                            (
                                                                SET(DATASET
                                                                    (
                                                                        [
                                                                            #SET(pos, 1)
                                                                            #SET(needDelim, 0)
                                                                            #LOOP
                                                                                #SET(temp, REGEXFIND('^([^,]+)', %'groupFields'%[%pos%..], 1))
                                                                                #IF(%'temp'% != '')
                                                                                    #IF(%needDelim% = 1) , #END
                                                                                    IF(TRIM(LEFT.%temp%) != '', %'temp'% + ':' + TRIM(LEFT.%temp%), '')
                                                                                    #SET(needDelim, 1)
                                                                                    #SET(pos, %pos% + LENGTH(%'temp'% + 1))
                                                                                #ELSE
                                                                                    #BREAK
                                                                                #END
                                                                            #END
                                                                        ],
                                                                        {UTF8 v}
                                                                    )(v != ''), v)
                                                            )
                                                        #SET(groupPos, %groupPos% + LENGTH(%'groupFields'% + 1))
                                                        #SET(needsOuterDelim, 1)
                                                    #ELSE
                                                        #BREAK
                                                    #END
                                                #END
                                            ],
                                            {UNSIGNED8 h}
                                        ), h);
                                    #SET(requiredHashCmd, 'HASH64(requiredSet)')
                                #ELSE
                                    #SET(requiredHashCmd, -1)
                                #END

                                otherSet := SET(DATASET
                                    (
                                        [
                                            #SET(groupPos, 1)
                                            #SET(needsOuterDelim, 0)
                                            #SET(bareFieldNames, REGEXREPLACE('%\\d+', %'otherFieldSpec'%, ''))
                                            #LOOP
                                                #SET(groupFields, REGEXFIND('^([^;]+)', %'bareFieldNames'%[%groupPos%..], 1))
                                                #IF(%'groupFields'% != '')
                                                    #IF(%needsOuterDelim% = 1) , #END
                                                    HASH64
                                                        (
                                                            SET(DATASET
                                                                (
                                                                    [
                                                                        #SET(pos, 1)
                                                                        #SET(needDelim, 0)
                                                                        #LOOP
                                                                            #SET(temp, REGEXFIND('^([^,]+)', %'groupFields'%[%pos%..], 1))
                                                                            #IF(%'temp'% != '')
                                                                                #IF(%needDelim% = 1) , #END
                                                                                IF(TRIM(LEFT.%temp%) != U8'', (UTF8)%'temp'% + U8':' + TRIM(LEFT.%temp%), U8'')
                                                                                #SET(needDelim, 1)
                                                                                #SET(pos, %pos% + LENGTH(%'temp'% + 1))
                                                                            #ELSE
                                                                                #BREAK
                                                                            #END
                                                                        #END
                                                                    ],
                                                                    {UTF8 v}
                                                                )(v != U8''), v)
                                                        )
                                                    #SET(groupPos, %groupPos% + LENGTH(%'groupFields'% + 1))
                                                    #SET(needsOuterDelim, 1)
                                                #ELSE
                                                    #BREAK
                                                #END
                                            #END
                                        ],
                                        {UNSIGNED8 h}
                                    )(h != HASH64([])), h);

                                SELF.required_hash_value := %requiredHashCmd%,
                                SELF.hash_values := IF(EXISTS(otherSet), otherSet, [-1]),
                                SELF := LEFT
                            )
                    );

                // Apply intra-field deletion neighborhood
                #UNIQUENAME(collapsedHashes)
                LOCAL %collapsedHashes% := NORMALIZE
                    (
                        %hashSets%,
                        %CreateNumericSetDeletionNeighborhood%(LEFT.hash_values, %myMaxEditDistance%),
                        TRANSFORM
                            (
                                {
                                    #$.Medley.ID_t      id,
                                    #$.Medley.Hash_t    hash_value
                                },
                                SELF.hash_value := HASH64(LEFT.required_hash_value, RIGHT.hash_value),
                                SELF := LEFT
                            )
                    );

                // Assign deduped data to an interim attribute
                LOCAL %interimLookupTable% := DEDUP(SORT(%collapsedHashes%, id, hash_value, LOCAL), id, hash_value, LOCAL)

                // Append the iterim attribute to our collection of attributes
                #IF(%fieldSpecIter% > 1)
                    #APPEND(combineLookupTableStmt, '+')
                #END
                #APPEND(combineLookupTableStmt, %'interimLookupTable'%)

                #SET(fieldSpecIter, %fieldSpecIter% + 1)
            #ELSE
                #BREAK
            #END
        #END

        #UNIQUENAME(finalResult);
        LOCAL %finalResult% :=
            #IF(%fieldSpecIsSet% = 0)
                %combineLookupTableStmt%
            #ELSE
                // Dedup the collected interim attributes
                DEDUP(SORT(%combineLookupTableStmt%, id, hash_value, LOCAL), id, hash_value, LOCAL)
            #END;

        RETURN %finalResult%;
    ENDMACRO;

    /**
     * Create ID <-> hash lookup index files from a lookup table (as returned
     * by CreateLookupTable()).  This function will overwrite existing
     * files with the same path.
     *
     * @param   lookupTable             The lookup table (as returned by
     *                                  CreateLookupTable()); REQUIRED
     * @param   id2HashIndexPath        Logical pathname of the ID->Hash
     *                                  index file; REQUIRED
     * @param   hash2IDIndexPath        Logical pathname of the Hash->ID
     *                                  index file; REQUIRED
     *
     * @return  An action that creates the two index files.
     *
     * @see     CreateLookupTable
     */
    EXPORT WriteIDLookupIndexes(DATASET(LookupTableLayout) lookupTable,
                                STRING id2HashIndexPath,
                                STRING hash2IDIndexPath) := FUNCTION
        RETURN PARALLEL
            (
                BUILD(Hash2IDLookupIndexDef(hash2IDIndexPath), lookupTable, OVERWRITE);
                BUILD(ID2HashLookupIndexDef(id2HashIndexPath), lookupTable, OVERWRITE);
            );
    END;

    /**
     * Performs a "fuzzy deduplication" from a lookup table (as if returned from
     * CreateLookupTable()).
     *
     * @param   lookupTable     Dataset to process
     *
     * @return  A new DATASET(CollapsedMatchingLayout).
     *
     * @see     CreateLookupTable
     */
    EXPORT DATASET(CollapsedMatchingLayout) CollapseLookupTable(DATASET(LookupTableLayout) lookupTable) := FUNCTION
        // Prep for rollup
        idLinksAsSet0 := PROJECT
            (
                lookupTable,
                TRANSFORM
                    (
                        {
                            RECORDOF(LEFT),
                            SET OF ID_t     id_set
                        },
                        SELF.id_set := [LEFT.id],
                        SELF := LEFT
                    )
            );

        // Group all shared IDs under the same hash value
        idLinksAsSet := ROLLUP
            (
                SORT(idLinksAsSet0, hash_value, id),
                TRANSFORM
                    (
                        RECORDOF(LEFT),
                        SELF.id_set := LEFT.id_set + RIGHT.id_set,
                        SELF := LEFT
                    ),
                hash_value
            );

        // ... and assign an initial matching_id to them
        matchedIDSets := PROJECT
            (
                idLinksAsSet,
                TRANSFORM
                    (
                        {
                            MatchingID_t        matching_id,
                            SET OF ID_t         id_set
                        },
                        SELF.matching_id := COUNTER,
                        SELF := LEFT
                    )
            );

        // Break the sets back out again, which basically gives us
        // a tiny matching_id/entity_id pair
        normalizedIDMatchID := NORMALIZE
            (
                matchedIDSets,
                DATASET(LEFT.id_set, {ID_t id}),
                TRANSFORM
                    (
                        {
                            MatchingID_t    matching_id,
                            ID_t            id,
                            MatchingID_t    related_matching_id
                        },
                        SELF.matching_id := LEFT.matching_id,
                        SELF.id := RIGHT.id,
                        SELF.related_matching_id := 0 // will assign later
                    )
            );

        // Initial 'glue' of rewriting one matching_id to a lower matching_id
        // on a per-entity ID basis; this forms a chain from a matching_id
        // to lower-valued IDs
        idSetsWithRelated := ITERATE
            (
                SORT(normalizedIDMatchID, id, -matching_id),
                TRANSFORM
                    (
                        RECORDOF(LEFT),
                        SELF.related_matching_id := IF(LEFT.id = RIGHT.id, LEFT.matching_id, 0),
                        SELF := RIGHT
                    )
            );

        // Further reduction:  Make sure each related_matching_id references
        // the lower values, in case of duplicates
        uniqueMatchIDPairs1 := TABLE
            (
                idSetsWithRelated(related_matching_id != 0),
                {
                    related_matching_id,
                    MatchingID_t    matching_id := MIN(GROUP, matching_id)
                },
                related_matching_id,
                MERGE
            );

        // Prepare this dataset for per-worker "chain walking" to minimize
        // matching_id for each related_matching_id; if the dataset is small
        // enough, distribute it to a single node so the subsequent LOOP has
        // less work to do
        uniqueMatchIDPairs2 := uniqueMatchIDPairs1(matching_id < related_matching_id);
        uniqueMatchIDPairsLarge := SORT(uniqueMatchIDPairs2, related_matching_id);
        uniqueMatchIDPairsSmall := DISTRIBUTE(uniqueMatchIDPairs2, 1);
        uniqueMatchIDPairs3 := IF(COUNT(uniqueMatchIDPairs2) < 1000000, uniqueMatchIDPairsSmall, uniqueMatchIDPairsLarge);
        uniqueMatchIDPairs4 := SORT(uniqueMatchIDPairs3, matching_id, related_matching_id, LOCAL);
        uniqueMatchIDPairs5 := PROJECT(uniqueMatchIDPairs4, MatchIDPairsRec);

        uniqueMatchIDPairs := uniqueMatchIDPairs5;

        reducedUniqueMatchIDPairs := LocallyReduceMatchPairs(uniqueMatchIDPairs);

        // Total reduction is not possible with a large reducedUniqueMatchIDPairs
        // because LocallyReduceMatchPairs() works only on a worker's local data
        // and chains could span workers; prepare the data for LOOP
        tempNormalizedIDMatchID := PROJECT
            (
                normalizedIDMatchID,
                TRANSFORM
                    (
                        {
                            RECORDOF(LEFT),
                            BOOLEAN         needsUpdating
                        },
                        SELF.needsUpdating := TRUE,
                        SELF := LEFT
                    )
            );

        // Repeatedly rewrite the original matching_id value, stopping only
        // when we no longer have chain links to walk
        reducedIDMatchID := LOOP
            (
                tempNormalizedIDMatchID,
                LEFT.needsUpdating,
                JOIN
                    (
                        ROWS(LEFT),
                        reducedUniqueMatchIDPairs,
                        LEFT.matching_id = RIGHT.related_matching_id,
                        TRANSFORM
                            (
                                RECORDOF(LEFT),
                                foundReduction := MATCHED(RIGHT) AND RIGHT.matching_id > 0;
                                SELF.matching_id := IF(foundReduction, RIGHT.matching_id, LEFT.matching_id),
                                SELF.needsUpdating := foundReduction,
                                SELF := LEFT
                            ),
                        LEFT OUTER, SKEW(1.0)
                    )
            );

        // Deduplicate
        uniqueIDMatchID := TABLE(reducedIDMatchID, {matching_id, id}, matching_id, id, MERGE);

        res := PROJECT(uniqueIDMatchID, CollapsedMatchingLayout);

        RETURN res;
    END;

    /**
     * Create MatchID <-> ID lookup index files from a deduplication result
     * (as returned by CollapseLookupTable()).  This function will
     * overwrite existing files with the same path.
     *
     * @param   matchingTable           The lookup table (as returned by
     *                                  CollapseLookupTable()); REQUIRED
     * @param   id2MatchIndexPath       Logical pathname of the ID->MatchID
     *                                  index file; REQUIRED
     * @param   match2IDIndexPath       Logical pathname of the MatchID->ID
     *                                  index file; REQUIRED
     *
     * @return  An action that creates the two index files.
     *
     * @see     CollapseLookupTable
     */
    EXPORT WriteMatchIDIndexes(DATASET(CollapsedMatchingLayout) matchingTable,
                               STRING id2MatchIndexPath,
                               STRING match2IDIndexPath) := FUNCTION
        RETURN PARALLEL
            (
                BUILD(Match2IDLookupIndexDef(match2IDIndexPath), matchingTable, OVERWRITE);
                BUILD(ID2MatchLookupIndexDef(id2MatchIndexPath), matchingTable, OVERWRITE);
            );
    END;

    /**
     * Returns all actions for building the deletion neighborhood index files
     * from a source dataset.  This single function macro bundles calls to
     * the following functions:
     *
     *      CreateLookupTable()
     *      CollapseLookupTable()
     *      WriteIDLookupIndexes()
     *      WriteMatchIDIndexes()
     *
     * @param   inFile              The dataset to process; REQUIRED
     * @param   idField             The unique identifier field within each
     *                              record; this is not a string; the field's
     *                              data type should match the ID_t definition
     *                              above; REQUIRED
     * @param   fieldSpec           A string describing how the dataset should
     *                              be processed; semi-colons are used to
     *                              delimit field groups, commas are used to
     *                              delimit fields; field names may optionally
     *                              have a suffix of '%N' where N is the
     *                              maximum edit distance to use when creating
     *                              an inter-field deletion neighborhood;
     *                              fields and field groups may have a
     *                              '&' prefix to indicate that the field or
     *                              field group should not be deleted when
     *                              constructing the intra-field deletion
     *                              neighhborhood; REQUIRED
     * @param   maxEditDistance     The maximum intra-field edit distance to
     *                              create; this is a positive integer; REQUIRED
     * @param   id2HashIndexPath    Logical pathname of the ID->Hash
     *                              index file; existing index will be
     *                              overwritten if present; REQUIRED
     * @param   hash2IDIndexPath    Logical pathname of the Hash->ID
     *                              index file; existing index will be
     *                              overwritten if present; REQUIRED
     * @param   id2MatchIndexPath   Logical pathname of the ID->MatchID
     *                              index file; existing index will be
     *                              overwritten if present; REQUIRED
     * @param   match2IDIndexPath   Logical pathname of the MatchID->ID
     *                              index file; existing index will be
     *                              overwritten if present; REQUIRED
     *
     * @return  An action that creates all index files and echoes three
     *          argument values to the workunit for tracking purposes
     *          (source_data_rec_count, frag_directive and
     *          frag_edit_distance).
     *
     * @see     CollapseLookupTable
     * @see     CollapseLookupTable
     * @see     WriteIDLookupIndexes
     * @see     WriteMatchIDIndexes
     */
    EXPORT BuildAllIndexes(inFile,
                           idField,
                           fieldSpec,
                           maxEditDistance,
                           id2HashIndexPath,
                           hash2IDIndexPath,
                           id2MatchIndexPath,
                           match2IDIndexPath) := FUNCTIONMACRO
        // Create the lookup table from the original data
        #UNIQUENAME(lookupTable);
        LOCAL %lookupTable% := #$.Medley.CreateLookupTable
            (
                inFile,
                idField,
                fieldSpec,
                maxEditDistance
            );

        // Perform the collapse (deduplication)
        #UNIQUENAME(coelescedDS);
        LOCAL %coelescedDS% := #$.Medley.CollapseLookupTable(%lookupTable%);

        #UNIQUENAME(createLookupIndexesAction);
        LOCAL %createLookupIndexesAction% := #$.Medley.WriteIDLookupIndexes
            (
                %lookupTable%,
                id2HashIndexPath,
                hash2IDIndexPath
            );

        #UNIQUENAME(createMatchIndexesAction);
        LOCAL %createMatchIndexesAction% := #$.Medley.WriteMatchIDIndexes
            (
                %coelescedDS%,
                id2MatchIndexPath,
                match2IDIndexPath
            );

        // Return the actions
        RETURN PARALLEL
            (
                OUTPUT(COUNT(inFile), NAMED('source_data_rec_count'));
                OUTPUT(fieldSpec, NAMED('frag_directive'));
                OUTPUT(maxEditDistance, NAMED('frag_edit_distance'));
                %createLookupIndexesAction%;
                %createMatchIndexesAction%;
            );
    ENDMACRO;

    /**
     * Given a dataset of IDs, return related IDs using previously-created
     * lookup indexes.
     *
     * @param   ids                     The dataset of IDs to search for;
     *                                  REQUIRED
     * @param   id2HashIndexPath        The logical pathname of the
     *                                  ID -> HashValue index, as defined
     *                                  by ID2HashLookupIndexDef();
     *                                  REQUIRED
     * @param   hash2IDIndexPath        The logical pathname of the
     *                                  HashValue -> ID index, as defined
     *                                  by Hash2IDLookupIndexDef();
     *                                  REQUIRED
     * @param   id2MatchIndexPath       The logical pathname of the
     *                                  ID -> MatchID index, as defined
     *                                  by ID2MatchLookupIndexDef();
     *                                  REQUIRED
     * @param   match2IDIndexPath       The logical pathname of the
     *                                  MatchID -> ID index, as defined
     *                                  by Match2IDLookupIndexDef();
     *                                  REQUIRED
     *
     * @return  DATASET(RelatedIDLayout) containing the results.
     *          ID values from the ids argument will appear in the
     *          given_id field of the result, with the search results
     *          appearing in the id field.
     *
     * @see     CreateLookupTable
     *          WriteIDLookupIndexes
     *          CollapseLookupTable
     *          WriteMatchIDIndexes
     */
    EXPORT DATASET(RelatedIDLayout) FindRelatedIDs(DATASET(IDLayout) ids,
                                                   STRING id2HashIndexPath,
                                                   STRING hash2IDIndexPath,
                                                   STRING id2MatchIndexPath,
                                                   STRING match2IDIndexPath) := FUNCTION
        id2HashIndex := ID2HashLookupIndexDef(id2HashIndexPath);
        hash2IDIndex := Hash2IDLookupIndexDef(hash2IDIndexPath);
        id2MatchIndex := ID2MatchLookupIndexDef(id2MatchIndexPath);
        match2IDIndex := Match2IDLookupIndexDef(match2IDIndexPath);

        // Get all neighborhood hash_values related to the given IDs
        gatheredHashes := JOIN
            (
                ids,
                id2HashIndex,
                LEFT.id = RIGHT.id,
                TRANSFORM
                    (
                        {
                            ID_t    given_id,
                            RECORDOF(RIGHT)
                        },
                        SELF.given_id := LEFT.id,
                        SELF := RIGHT
                    )
            );

        // Find all IDs (immediately) linked to the given IDs
        // through neighborhood hash_values
        gatheredIDs := JOIN
            (
                gatheredHashes,
                hash2IDIndex,
                LEFT.hash_value = RIGHT.hash_value,
                TRANSFORM
                    (
                        {
                            ID_t    given_id,
                            RECORDOF(RIGHT)
                        },
                        SELF.given_id := LEFT.given_id,
                        SELF := RIGHT
                    )
            );

        // Lookup match_id values for all gathered IDs
        initialMatches := JOIN
            (
                gatheredIDs,
                id2MatchIndex,
                LEFT.id = RIGHT.id,
                TRANSFORM
                    (
                        {
                            ID_t    given_id,
                            RECORDOF(RIGHT)
                        },
                        SELF.given_id := LEFT.given_id,
                        SELF := RIGHT
                    )
            );

        // Collect all IDs associated with collected match_id values
        relatedMatches := JOIN
            (
                initialMatches,
                match2IDIndex,
                LEFT.matching_id = RIGHT.matching_id,
                TRANSFORM
                    (
                        {
                            ID_t    given_id,
                            RECORDOF(RIGHT)
                        },
                        SELF.given_id := LEFT.given_id,
                        SELF := RIGHT
                    )
            );

        // There are likely a lot of duplicates
        dedupedRelated := TABLE(relatedMatches, {given_id, id}, given_id, id, MERGE);

        RETURN PROJECT(dedupedRelated, RelatedIDLayout);
    END;

    /**
     * Given a lookup table (such as the result of CreateLookupTable()),
     * return related IDs using previously-created lookup indexes.
     *
     * @param   lookupTable             The lookup table (as returned by
     *                                  CreateLookupTable()); REQUIRED
     * @param   hash2IDIndexPath        The logical pathname of the
     *                                  HashValue -> ID index, as defined
     *                                  by Hash2IDLookupIndexDef();
     *                                  REQUIRED
     * @param   id2MatchIndexPath       The logical pathname of the
     *                                  ID -> MatchID index, as defined
     *                                  by ID2MatchLookupIndexDef();
     *                                  REQUIRED
     * @param   match2IDIndexPath       The logical pathname of the
     *                                  MatchID -> ID index, as defined
     *                                  by Match2IDLookupIndexDef();
     *                                  REQUIRED
     *
     * @return  DATASET(IDLayout) containing the results.  The IDs
     *          returned will be deduplicated.
     *
     * @see     CreateLookupTable
     *          WriteIDLookupIndexes
     *          CollapseLookupTable
     *          WriteMatchIDIndexes
     */
    EXPORT DATASET(IDLayout) FindRelatedIDsFromLookupTable(DATASET(LookupTableLayout) lookupTable,
                                                           STRING hash2IDIndexPath,
                                                           STRING id2MatchIndexPath,
                                                           STRING match2IDIndexPath) := FUNCTION
        hash2IDIndex := Hash2IDLookupIndexDef(hash2IDIndexPath);
        id2MatchIndex := ID2MatchLookupIndexDef(id2MatchIndexPath);
        match2IDIndex := Match2IDLookupIndexDef(match2IDIndexPath);

        // Find all IDs (immediately) linked to the lookup table IDs
        // through neighborhood hash_values
        hashMatches := JOIN
            (
                lookupTable,
                hash2IDIndex,
                LEFT.hash_value = RIGHT.hash_value,
                TRANSFORM(RIGHT),
                LIMIT(0)
            );

        initialIDList := TABLE(hashMatches, {id}, id, MERGE);

        // Lookup match_id values for all gathered IDs
        initialMatches := JOIN
            (
                initialIDList,
                id2MatchIndex,
                LEFT.id = RIGHT.id,
                TRANSFORM(RIGHT),
                LIMIT(0)
            );

        initialMatchIDList := TABLE(initialMatches, {matching_id}, matching_id, MERGE);

        // Collect all IDs associated with collected match_id values
        relatedMatches := JOIN
            (
                initialMatchIDList,
                match2IDIndex,
                LEFT.matching_id = RIGHT.matching_id,
                TRANSFORM(RIGHT),
                LIMIT(0)
            );

        // There are likely a lot of duplicates
        dedupedRelated := TABLE(relatedMatches, {id}, id, MERGE);

        RETURN PROJECT(dedupedRelated, IDLayout);
    END;

END;
