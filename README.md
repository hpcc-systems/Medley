# Medley

## What Is It?

Medley is a library that supports searching for "similar" records in a dataset.

This concept of "similar" is best defined via analogy:  Two words can be similar
if they differ in spelling by only a letter or two.  This is how many spell-checkers
work:  Finding real words that differ from what you typed by only
a few letters.  Extending that idea, Medley finds similar dataset records
by examining field values and finding records where only a few field values
are different.

The module [Medley.ecl](Medley.ecl) provides the following features:

- UTF-8 support
- Creation of search indexes, linked back to entity ID values
- Fuzzy matching within the source dataset, linking similar IDs
- Searching for related IDs, given a dataset of IDs
- Searching for related IDs, given a dataset of data mimicking the source dataset

## License
This software is [licensed](LICENSE.txt) under the Apache v2 license.

## Dependencies

Medley supports UTF-8 via the ICU (International Components for Unicode) library.
The runtime version of this library is used by the HPCC Systems platform code,
but Medley requires the header files as well since you're compiling new code
using Unicode support.  If you receive an error indicating that the file
unicode/unistr.h cannot be found, then you need to install a library package.
For either RHEL/CentOS or Debian operating systems, that package is libicu-dev.
At minimum, you need to install it on the node that compiles your ECL code
(the node running eclccserver).

## Versions

The ECL module itself can be inspected for version information at compile time.
The following attributes are all exported:

	UNSIGNED1 VERSION_MAJOR
	UNSIGNED1 VERSION_MINOR
	UNSIGNED1 VERSION_POINT
	STRING    VERSION_STRING

|Version|Notes|
|:----:|:-----|
|0.5.0|Initial public release|
|0.6.0|Support for multiple field directives in a single build|
|0.6.1|Rename offensive terms; replace expensive self-join with a rollup; skip non-required fields containing empty strings when computing hashes|
|0.6.2|Rearrange ECL #body declarations in embedded C++ functions|

## Example Code

The Example directory contains BWRs for creating Medley indexes, analyzing deduplication
results, and querying the indexes for both related entity IDs as well as fuzzy searching.

## Theory of Operations

The basic concept implemented here is called "deletion neighborhoods".
It is a term coined in an academic paper written by Thomas Bocek, Ela Hunt,
and Burkhard Stiller from the University of Zurich, titled ["Fast Similarity
Search in Large Dictionaries"](https://fastss.csg.uzh.ch/ifi-2007.02.pdf).
The work described there was expanded in a paper written by Daniel Karch,
Dennis Luxen, and Peter Sanders from the Karlsruhe Institute of Technology,
titled ["Improved Fast Similarity Search in Dictionaries"](https://arxiv.org/abs/1008.1191v2).
Both of these papers deal with efficient searching for similar string values, given a query string.

This ECL module takes the concept of a deletion neighborhood and applies it
to dataset records.  Once you read the papers, the executive summary is
straightforward:  instead of working with words composed of characters, we
work with records composed of fields.  Both string- and record-based
deletion neighborhood techniques are used here, as both offer powerful
capabilities when combined.

The data that this module is designed to work with is most easily described
as "entity data" -- usually thought of as, "each record describes a single
person, place, or thing."  More generally, any kind of data will work just
fine as long as each record contains a unique entity ID field and then
more fields that add information about that ID.  IDs can be duplicated
in the dataset, so long as the information associated with them belong
to the right IDs (in other words, don't reuse the entity IDs for
different entities).

The concept of applying a deletion neighborhood technique to records like
you would strings is easy to grasp, but there are a few tweaks that improve
the results:

1) Field values, in isolation, sometimes don't offer enough actual
information to be worthwhile when it comes to searching (or to
put a different way, discriminating between records).  The rule of
thumb is, the lower the cardinality in a field the less unique
information it adds.  An example is a field containing abbreviations
of U.S. states.  A value of "TX" does not necessarily help
discriminate one record from another (though it might; it depends
on the use case).  However, if you pair this field with another
field, say "city_name", then the combined information helps
record discrimination tremendously.  "Austin" as a city name, by
itself, can refer to a number of different cities in the U.S. but
when paired with the state abbreviation "TX" it suddenly acquires
greater precision.  This module therefore offers a method for
combining fields into "field groups" and treating those groups
as if they were a single value.

2) One of the more powerful aspects of deletion neighborhoods is that
the process of iteratively deleting units (characters from strings,
or field values from records) is blind.  We don't actually care
about what we're deleting.  But that is not always true when it
comes to fields (or field groups).  When working with records
that contain location data, for instance, it may be super important
that we never ignore the postal code from the record, because if we
do then we run the risk of matching records that are geographically
distant.  This module therefore offers the ability to designate
fields and field groups as "required" which has the effect of
making them not deletable.

3) The idea of creating deletion neighborhoods against string values
is powerful and should not require an extra step to use.  Therefore,
this module offers the ability to expand any field into a deletion
neighborhood prior to creating the deletion neighborhood for the
record as a whole.  This has the effect of duplicating a given
record a number of times, with each containing a slightly different
"version" of that field value.  As a bonus, any number of fields
may be expanded this way, and each may cite a different edit
distance (you did read the papers, right?).  The practical side
of this functionality is the ability to "fuzzy match" on these
fields even while performing the record-based similarity matching.
It is also worth noting that the module supports UTF-8 strings,
not just plain ASCII strings.  (Side note:  If you are interested
in just fuzzy-matching strings and not whole-record fuzzy matching, see
[FuzzyStringSearch.ecl](https://github.com/dcamper/Useful_ECL/blob/master/FuzzyStringSearch.ecl).)

One of the extremely cool features of record-level deletion neighborhoods is
the ability to solve a normally hard-to-code use case:  Given a search form
in a browser, the user is presented with somes fields to fill in (these just
happen to correspond to the fields you previously indexed using this module).
The requirement is that the user should basically fill in as many of the
fields as she can and the system should locate related records.

Using this module, you can accommodate this type of searching by adjusting
the field group-level maximum edit distance (MaxED) when building the index.
The simple formula is:

     MaxED = (total number of fields) - (minimum number of entered fields)

If the requirements say that the user needs to fill out only one of ten
fields presented, then your MaxED value is 9.  If the user needs to fill
out any six fields, then the MaxED value is 4.  When searching, set the MaxED
value to zero to prevent over-fuzzing the search parameters.

The uber-cool part of this is two-fold:

1) Other than different MaxED values, there is no change to the
indexing code.
2) Other than different MaxED values, there is no change to the
searching code.

Do keep in mind that the larger the edit distance, the bigger the indexes
and the greater the chance for seeing a false positive search result.

"Larger indexes" has been bandied about a few times.  What does that really
mean?  You can compute the number of records created by a deletion
neighborhood with the following pseudocode (note that 'fact' means
computing the factorial of the argument):

     for (r = 1; r <= MaxED; r++)
         numRecs += (fact(n) / (fact(r) * fact(n - r)));

Where n = the number of fields in your record.  n could also be the number
of characters in a string, because this equation works just as well for
strings.  The reason this is a summation is because deletion neighborhoods
in effect store everything from edit distance zero (an exact copy of the
input) up to MaxED.

Note that the equation gives you the result if you have ONE record (or
string).  If you have many records to process, multiple that count by
numRecs.  If are using the equation to for a string field, use the
average length of the string value for n, then multiply by numRecs.

To give you some ideas of the scale of this "record explosion" here are
the results of expanding one record with 10 fields (or one string that
is 10 characters in length), with different MaxID values:

     MaxED   numRecs
     ----------------
       1       11
       2       56
       3      176
       4      386
       5      638
       6      848
       7      968
       8     1013
       9     1023

To reiterate:  That is the number of index records generated from ONE input
record.

There is another consideration regarding the index files this module
creates:  Disk space.  In HPCC Systems, indexes are naturally compressed.
This is good.  Unfortunately, while these indexes all have simple layouts
(a pair of numbers), they are mostly *random* numbers.  They do not
compress well at all.  The ones with ID values first tend to compress better,
but none of them are outstanding.  This in no way hurts performance, but it
is a consideration for storage.

## How To Use Medley

What follows is the suggested basic "flow" for using this module.

1) Examine your dataset and identify the search fields -- those fields
that contain data that help discriminate between records.  The fewer
the fields, the smaller the indexes and the faster the process
will run.  But hey, if you have a monstrous HPCC Systems cluster
feel free to go nuts.

2) Identify fields that should be grouped together, if any.

3) Identify fields that should be expanded with their own deletion
neighborhoods to aid fuzzy matching, if any.  Note that if your
data is already heavily normalized, you may not have any such
fields.

4) Create a field directive string or a set of strings (see the section titled
[Field Directive Formatting](#field_directive_formatting), below)
using the information from steps 1-3.

5) Decide on the record (or field group-level) maximum edit distance
you want to use.  Remember that higher values produce more index
records, take longer to process, and will return more "hits" when
searching but they will also produce more false positives.  Note
that you can set the maximum edit distance to zero.  If you do,
no field-based deletion neighborhood will be built, turning the
indexes into a fast methhod for multi-field exact matches.

6) Call BuildAllIndexes() to create all of the indexes.

After six steps, you now have all the data you need for Fun Searching.
That data is composed of four simply-formatted but probably very large
INDEX files.

**Fun Search Scenario #1:** Given one or more of your unique IDs, find all
the related IDs.  This is pretty straightforward:

1) Stuff your IDs into a DATASET(IDLayout) and call the
FindRelatedIDs() function.  You will need to pass in the logical
pathnames for some of the index files as well.  What you get back
is a set of records containing one of your original IDs and
a related ID (RelatedIDLayout format).

**Fun Search Scenario #2:**  Given a set of information that mimics the data
you have indexed -- basically a populated one-record dataset in the
same format as your original data -- and find the IDs of the matching
records.

1) Create a dataset with a layout that includes at least the fields
used when creating the deletion neighborhood indexes and populate it
with your search values.  Make sure to include the unique ID field, even
though you probably don't have a value for it.  You can have more than
one record in this dataset if needed.

2) Pass this created dataset to CreateLookupTable() along with the
field directive string you used when creating the lookup
indexes, and the field group-level edit distance.  You will get
back a lookup table of hash codes.

3) Pass the lookup table to the FindRelatedIDsFromLookupTable()
function.  What you get back will be a simple list of IDs
(in IDLayout format) that are similar to the data from step #1.

**Fun Search Scenario #3:** Like #2, but you are satisfying the use case
of "find records given up anywhere from N to M number field values"
(where M is the actual number of fields you have indexed).

1) Create a dataset with a layout that includes at least the fields
used when creating the deletion neighborhood indexes and populate it
with your search values.  In this layout, use UTF8 as the data type
for every field, but use the same names.  Where the user does not
supply a value for a field, make sure that field's value is an empty
string.  Make sure to include the unique ID field, even
though you probably don't have a value for it.

2) Pass this created dataset to CreateLookupTable() along with the
field directive string you used when creating the lookup
indexes, and use zero for the field group-level edit distance.  You will get
back a lookup table of hash codes.

3) JOIN those hash codes against the index defined by
`Medley.Hash2IDLookupIndexDef(Medley.HASH2ID_LOOKUP.PATH_ROXIE)`,
basically filtering that index by the lookup table you computed.  The JOIN
would look something like this:

        hashMatches := JOIN
            (
                lookupTable,
                hash2IDIndex,
                LEFT.hash_value = RIGHT.hash_value,
                TRANSFORM(RIGHT),
                LIMIT(0)
            );

The result of that JOIN will be a dataset with this layout:

        LookupTableLayout := RECORD
            ID_t                id;
            Hash_t              hash_value;
        END;

The results will be the matches against your data, and the `id` field
is your entity ID.  Now you can look up those IDs in your master file
to retrieve the original data.

<a name="field_directive_formatting"></a>
## Field Directive Formatting

The field directive (the 'fieldSpec' parameter in Medley's
CreateLookupTable() function macro) is a single ```STRING``` or
```SET OF STRING``` argument defining how the input dataset
should be parsed while creating lookup and search neighborhoods.  Multiple
directives can be supplied via the ```SET OF STRING``` form, with the
effect of creating an OR condition between them.

Each field directive string is a semi-colon delimited string, with each
element defining a "field group".  A field group is a comma-delimited list
of field names from the input dataset.  A field group may contain only one
field name.  The unique ID field should not normally be included in any
field group.   Individual fields may appear multiple times, but you should
think carefully about the impact of doing so.

Individual fields may be expanded with their own deletion neighborhoods.
To indicate such an expansion, append the suffix of '%N' to the field's
name, where N is the maximum edit distance for the deletion neighborhood.
Normally, N will be either 1 or 2 (larger maximum edit distances create
considerably larger lookup tables and the result may cause too many
false positive search results).  If a field appears more than once in a
field directive and any of them have a %N suffix, then all
occurrences of that field will be be expanded with a maximum edit distance
of MAX(N).

Field group-level deletion neighborhoods are all about systematically
ignoring certain field groups when creating hash values.  To indicate that
a field group should not be ignored -- that it is, required -- prepend
the entire field group with a '&' character.  The '&' character should
appear as a prefix of the first field name in the field group.

Practical example:  Let's assume you are working with this data structure:

     RECORD
         UNSIGNED6    id;
         UTF8         fname;
         UTF8         lname;
         UTF8         street;
         UTF8         city;
         UTF8         state;
         STRING       postal;
     END;

Remember, the entity ID field is NOT part of your directive.

If you want to consider all of these fields independently, without grouping
or creating any string-based deletion neighborhoods, the field
directive is a simple semi-colon delimited list of the six fields:

     'fname;lname;street;city;state;postal;'

Note that a "field group" is defined as "one or more fields" so you have a
field directive defining six field groups, even though each field
group has only one field in it.

Now let's say that in the interest of precision, you want to consider the
city, state, and postal fields together:  Don't break them up, and don't
delete one of those independently.  Those fields become a "field group"
and are comma-delimited items:

     'fname;lname;street;city,state,postal;'

You now have four field groups:

     fname
     lname
     street
     city, state, postal

Let's further assume that the city/state/postal field group is important
and that you never want it omitted from the index creation (recall that
creating deletion neighborhoods BLINDLY deletes items).  The way you
designate a field group as required is to prepend the entire group (meaning,
the first field name in the group) with an ampersand character:

     'fname;lname;street;&city,state,postal;'

You still have four field groups, but only three of them will participate
in the deletion neighborhood.  If your MaxED is 1, that means indexes will
be created for the following combinations of field values:

     fname, lname, street, city/state/postal
     lname, street, city/state/postal
     fname, street, city/state/postal
     fname, lname, city/state/postal

That combination of values is what you will be matching on.

You could also indicate that you want string-based deletion neighborhoods
created for certain fields (not field groups).  For instance, if your data
has not been thoroughly cleaned or if you will need to account for typos
and such when searching, you may need that extra "fuzziness" to match
records correctly.  String-based deletion neighborhoods are designated
within the field directive by adding a suffix of '%N' to the field's
name, where N is the MaxED you want.  In our example, let's say that you
want to be able to find first names with up to one character different
(MaxED = 1) and the street part of the address with up to two characters
different (MaxED = 2).  The directive will become:

     'fname%1;lname;street%2;&city,state,postal;'

This would cause every record in your dataset to be duplicated with subtle
variations in the fname and street values, exploding the size of the data
temporarily.  The field-level deletion neighborhood is the same, though:

     fname, lname, street, city/state/postal
     lname, street, city/state/postal
     fname, street, city/state/postal
     fname, lname, city/state/postal

It's just that there will be many more of those records processed.

As an example of using multiple field directives, let us take our example
and assume that we want to match records using two different criteria:

     'fname;lname;postal;'
     'lname;city,state,postal;'

Those two criteria are considered OR'd together when it comes to matching.
All of the other formatting directives and limitations are valid for each
directive.  To supply them, simply submit them in a ```SET OF STRING```
data type rather than a simple ```STRING```:

     ['fname;lname;postal;', 'lname;city,state,postal;']
