import re
import sys
import yaml
import argparse
import json
import copy
import gzip
from base64 import b64encode, b64decode

from datetime import datetime
import time
import decimal


class SimpleRecordFormat(object):
    """ Generates record format objects for text data.

    SimpleRecordFormat can be extended to create custom record format
    objects which provide a simple interface for parsing, manipulating
    and representing structured data.

    Create a custom record format by defining its specification
    in a subclass or a yaml document.

    e.g.
    class myformat(SimpleRecordFormat) :
    def __init__(self) :
	fields = ['f1','f2']
	SimpleRecordFormat.__init__(self, fieldList=fields)

    or
    class myformat(SimpleRecordFormat) :
       def __init__(self) :
  	  __spec__ = [ {'f1': ['NONNULL'] },
		  {'f2': ['INTEGER'] }]
	  SimpleRecordFormat.__init__(self, spec=__spec__)

    or
    name: myformat
    record:
       - f1
       - f2

    All examples define a specification for myformat, with fields
    (instance attributes) f1 and f2.
    Using "__spec__" provides the ability to define properties for each field.
    Fields are not bound to a type (str,int etc.) i.e. no type checking is
    performed during assignment. However, field (attribute) values can be
    tested for corresponding field properties.

    e.g.
    valid, errors = myformat.validate()
    valid is boolean.

    SimpleRecordFormat instances handle delimited data by default, fixed
    length layouts can be parsed by specifying a length for each field and
    "recordType=fixed_length".

    e.g. class myformat(SimpleRecordFormat) :
     def __init__(self) :
	 __spec__ = [ {'f1(10)': ['NONNULL']},
		  {'f2(50)': ['NONNULL']} ]
	 SimpleRecordFormat.__init__(self, recordType='fixed_length', spec=__spec__)


    Methods:

    - fromString(text,separator):

    - fromList(list)

    - toString(separator):

    - validate():


    Interfaces:
    - SimpleRecordFactory(yaml_file)

    - getRecordReader(yaml_file)

    - getRecordReaderClass(yaml_file)


    Yaml specs:
    e.g. name: deals
         delimiter: \x01
         record:
         - deal_id: NONNULL
         - salesforce_id
         - division_id:
               - NONNULL
               - DECIMAL

         name: giftcard_incomm
         type: fixed_length
         record:
         - TransactionID(10)
         - RemittanceLogType(12)
         - ReconciliationStatus(2)
         - TransactionDate(8)

    For multiple formats in a single yaml file, set 'collection'
    as the top level block and define individual formats as a
    sequence of mappings.
    e.g.
    ('myfile.yml')

    collection:
       - name: rec1
         delimiter: \t
         record:
             - f1

       - name: rec2
         delimiter: \t
         record:
             - f2

    """

    def __init__(self, **kwargs):

        def __handleSpec(spec, spec_type):
            fieldList = []
            fieldLengths = []

            if spec_type == 'delimited':
        	    fieldList = [list(fieldSpec.keys())[0]
        	                      for index, fieldSpec in enumerate(spec)]

            elif spec_type == 'fixed_length':
                for index, fieldSpec in enumerate(spec):
                    field_name, length = re.search(
                        '(.*)\(([0-9]+)\)', list(fieldSpec.keys())[0]).groups()
                    fieldList.append(field_name)
                    fieldLengths.append(int(length))

            return (fieldList, fieldLengths)

        self.__name__ = kwargs.get('name', self.__class__.__name__)
        self.__type__ = kwargs.get('recordType', 'delimited')

        self.__spec__ = kwargs.get('spec', None)
        self.__fieldList__ = kwargs.get('fieldList', [])
        self._delimiter = kwargs.get('separator', None)
        self._trailing_nulls = kwargs.get("trailing_nulls", False)

        if self.__spec__:
            self.__fieldList__, self.__fieldLengths__ = __handleSpec(
                self.__spec__, self.__type__)

        self.__numFields__ = len(self.__fieldList__)

        for field in self.__fieldList__:
            setattr(self, field, '')

    def get(self, attribute):
        return getattr(self, attribute)

    def set(self, attribute, value):
        setattr(self, attribute, value)

    def makeCopy(self):
        return copy.deepcopy(self)

    def createJSONSchema(self, **kwargs):

        self.__type__ = 'json'
        json_text = kwargs.get('json', None)
        sublevel = kwargs.get('sublevel', None)

        if not sublevel:
            self.parsed_json = json.loads(json_text)
            json_dict = self.parsed_json
        else:
            json_dict = sublevel

        for key, value in list(json_dict.items()):
            if isinstance(value, dict):
                subobj = SimpleRecordFormat(name=key, recordType='blank')
                subobj.createJSONSchema(sublevel=value)
                setattr(self, key, subobj)
                self.__fieldList__.append(subobj.__name__)

            elif isinstance(value, list):
                elements = []
                for v in value:
                    subobj = SimpleRecordFormat(name=key, recordType='blank')
                    subobj.createJSONSchema(sublevel=v)
                    elements.append(subobj)

                setattr(self, key, elements)
                self.__fieldList__.append(subobj.__name__)

            else:
                setattr(self, key, value)
                self.__fieldList__.append(key)

    def fromList(self, valueList):
        """uses elements of a list to populate field values."""
        if len(valueList) < self.__numFields__:
            raise ValueError(
                "record %s | Not enough fields input" % self.__name__)

        if len(valueList) > self.__numFields__:
            raise ValueError(
                "record %s | Too many fields in input" % self.__name__)

        self.valueList = valueList

        for index, field in enumerate(self.__fieldList__):
            setattr(self, field, self.valueList[index])

    def fromString(self, text, **kwargs):
        """parses text into fields using separator."""

        separator = kwargs.get('separator', None)

        if self.__type__ == 'delimited':
            separator = self._delimiter if not separator else separator

            if not separator:
                raise DefinitionError(
                    "%s | separator not defined for delimited record" % self.__name__)

            self.valueList = text.split(separator)
            if self._trailing_nulls:
                chompedList = self.valueList[0:self.__numFields__]

                self.valueList = chompedList

        elif self.__type__ == 'fixed_length':
            self.valueList = []
            offset = 0
            index = 0
            input_length = len(text)

            for field_len in self.__fieldLengths__:
                index = offset + field_len
                if index > input_length:
                    raise ValueError(
                        'record %s | unable to parse input string: %s' % (self.__name__, text))
                self.valueList.append(text[offset: index])
                offset = index

        if len(self.valueList) != self.__numFields__:
            raise ValueError(
                'record %s | unable to parse input string: %s' % (self.__name__, text))

        for index, field in enumerate(self.__fieldList__):
            setattr(self, field, self.valueList[index])

    def toString(self, **kwargs):
        """returns a string of field values delimited by separator."""
        separator = self._delimiter if not kwargs.get(
            'separator') else kwargs.get('separator')

        if not separator:
            raise Exception(
                "%s | separator not defined for delimited record" % self.__name__)

        replaceDelim = kwargs.get('replaceDelimiter', None)
        enforceLen = kwargs.get('enforceLength', False)
        # valueList = [ str(getattr(self, field)) for field in self.__fieldList__ ]

        valueList = [repr(getattr(self, field))
                          for field in self.__fieldList__]
        valuesList = ['']*len(self.__fieldList__)

        for index, field in enumerate(self.__fieldList__):
            value = getattr(self, field)
            if value == '' or value is None:
                value = ''
            elif not isinstance(value, str):
                value = str(value)
            else:
                pass

            if replaceDelim:
                value = re.sub('\%s' % separator, replaceDelim, value)
                if enforceLen:
                    length = self.__spec__[index][field]['length'] if self.__spec__[
                        index][field]['length'] else len(value)
                    value = value[:length]

            valueList[index] = value

        outputString = separator.join(valueList)

        if kwargs.get('replaceNewlines'):
            outputString = re.sub('\n', ' ', outputString)
            outputString = re.sub('\r', ' ', outputString)

        return outputString

    def addField(self, field): 
        """
        Add field
        """
        self.__fieldList__.append(field)
        setattr(self, field, '')
        # setattr(self, field, '')


    def prettify(self):
        """
        Prettify
        """
        if self.__type__ == 'json':
            self.prettifyJson()

        else:
            valueList = [getattr(self, field) for field in self.__fieldList__]
            for index, field in enumerate(self.__fieldList__):
                print(field + ': "' + str(valueList[index]) + '"')



    def prettifyJson(self,sublevel=None) :
        self.__fieldList__.sort()
        level = 0 if not sublevel else sublevel
        for index, field in enumerate(self.__fieldList__) :
            value = getattr(self, field)

            if isinstance(value, self.__class__) :
                print('\t'*level + '%s :'%(field))
                value.prettifyJson(sublevel=level+1)

            elif isinstance(value, list) :
                print('\t'*level +'%s:'%(field))
                for v in value :      
                    print('\t'*(level) + '    [')
                    v.prettifyJson(sublevel=level+1)
                    print('\t'*(level) + '    ]')

                print('\t'*level + ']')
            else : 
                print('\t'*level + field + ': "' + str(value) + '"')


    def copy(self, source) :
        if self.__name__ != source.__name__ :
            raise TypeError('type mismatch - cannot copy "' + self.__name__ + '" to "' + source.__name__ + '"')

        for field in source.__fieldList__ :
            value = getattr(source, field)
            setattr(self, field, value)


    def equals(self, other, **kwargs) :
        ignore = kwargs.get('ignore_fields', [])

        if self.__name__ != other.__name__ :
            raise Exception('Cannot compare instances of different formats: %s, %s'%self__name__, other.__name__)

        valueList = [ getattr(self, field) for field in self.__fieldList__ ]
        othervalues = [ getattr(other, field) for field in other.__fieldList__ ]

        diff = [ self.__fieldList__[index] for index, value in enumerate(valueList) if value != othervalues[index] and self.__fieldList__[index] not in ignore]
        
        return ((True, []), (False, diff))[ len(diff) > 0 ]


    def copyCommonFields(self, source) :
        for field in source.__fieldList__ :
            if field in self.__fieldList__ :
                value = getattr(source, field)
                setattr(self, field, value)
        
        return


class ReaderTemplate(SimpleRecordFormat):
    def __init__(self, **kwargs) :
        recordType = kwargs.get('recordType', 'delimited')
        name = kwargs.get('name', None)
        spec = kwargs.get('spec', None)
        separator = kwargs.get('separator', None)

        if recordType == 'delimited' : 
            name = 'simpleDelimited' if not name else name


        elif recordType == 'fixed_length' :
            name = 'simpleFixedLength' if not name else name
   
        else :
            raise DefinitionError('Unknown type %s'%recordType)

        SimpleRecordFormat.__init__(self, name=name, recordType=recordType, spec=spec, separator=separator)


def parseYamlSpec(name, spec_d) :

    readerSpec = spec_d['record'] 
        
    delimiter = spec_d.get('delimiter', None)
    recordType = spec_d.get('type', 'delimited')
    fieldLengths = spec_d.get('field_lengths', None)
    if fieldLengths and not isinstance(fieldLengths, list) :
        raise Exception('%s: field_lengths must be of type list, provided type is %s'%(name, type(fieldLengths)))
    
        if len(fieldLengths) != len(readerSpec) :
            raise Exception('%s: number of elements in field_lengths %d does not match number of fields %d'%(name, len(fieldLengths), len(readerSpec))) 

    if delimiter :
        delimiter = delimiter.decode("string-escape")

    normSpec = []

    for index, field in enumerate(readerSpec) :
        if isinstance(field, dict) :
            normSpec.append(field)
        else :
            length = int(fieldLengths[index]) if fieldLengths and fieldLengths[index] else None

            tmp_d = { field : {'length': length} }
            normSpec.append(tmp_d)


    spec_args = {'name':name, 
                'recordType':recordType, 
                'spec':normSpec, 
                'separator':delimiter}          

    return spec_args


def getRecordReader(yml_file, **kwargs) :
    """returns a SimpleRecordFormat template instance created from a "spec" defined in yaml."""
    spec_d = yaml.load(open(yml_file, 'r'))

    readerName = spec_d['name']
    readerSpec = spec_d['record'] 

    delimiter = spec_d.get('delimiter', None)
    recordType = spec_d.get('type', 'delimited')

    if delimiter :
        delimiter = delimiter.decode("string-escape")

    normSpec = []
    for field in readerSpec :
        if isinstance(field, dict) :
            normSpec.append(field)
        else :
            tmp_d = { field : []}
            normSpec.append(tmp_d)

    reader = ReaderTemplate(name=readerName, recordType=recordType, spec=normSpec, separator=delimiter)
    return reader


    def getRecordReaderClass(yml_file) :
        """returns a SimpleRecordFormat template class created from a "spec" defined in yaml."""
        return getRecordReader(yml_file).__class__


class SimpleRecordFactory() :
    """implements a factory for generating SimpleRecordFormat instances from
       formats defined in yaml.
       - initialize with a yaml file containing record format(s).
       - retrieve new SimpleRecordFormat instance from factory
         by calling getRecord(<record format name>)

         e.g.
             myfactory = SimpleRecordFactory('myformats')
             myrecord = myfactory.getRecord('myrecord')

    """
    def __init__(self, *args) :

        self.format_args = {}
        self.formats = []

        for arg in args :
            if isinstance(arg, dict) :
                spec = arg
            else :
                spec = yaml.load(open(arg, 'r'))
            
            self.formats.extend(list(spec.keys()))
            for name, format in list(spec.items()) :
                self.format_args[name] = parseYamlSpec(name, format)
        
        self.counters = dict([(name, 0) for name in self.formats])


    def addFormat(self, format) :
        for name, spec in list(format.items()) :
            self.formats.append(name)
            self.format_args[name] = parseYamlSpec(name, spec)
                            
    def getRecord(self, rec_name) :
        
        return SimpleRecordFormat(name=self.format_args[rec_name]['name'],
                                  recordType=self.format_args[rec_name]['recordType'],
                                  spec=self.format_args[rec_name]['spec'],
                                  separator=self.format_args[rec_name]['separator'])


    def parseRecords(self, stream, **kwargs) :
        returnRaw = kwargs.get('returnRaw', False)
        format = kwargs.get('format', None)
        getFormat = kwargs.get('getFormat', None)
        kV = kwargs.get('keyValue', False)
        kVs = kwargs['keyValueSep'] if kV else None
        kFmt = kwargs.get('keyFormat', None)
        decodeV = kwargs.get('decodeValue', None)
        returnParseErrors = kwargs.get('returnParseErrors', None)

        inputCount = 0
        errorLimit = int(kwargs.get('errorLimit', 0))
        errorCount = 0
        
        if decodeV and decodeV != 'base64' :
            raise NotImplementedError('%s encoding not supported'%decodeV)

        if not format and not getFormat :
            raise Exception('missing required kwargs: specify format or getFormat')
	
        for line in stream :
            inputCount += 1
            if getFormat :
                format = getFormat(line)

            if kV :
                key, sep, value = line.partition(kVs) 
            else :
                key, value = (None, line)
            
                if decodeV :
                    value = b64decode(value)

                rec = self.getRecord(format)
            try :
                    rec.fromString(value)
            except ValueError as e:
                errorCount += 1
                if returnParseErrors :
                    error_rec = SimpleParseError()
                    error_rec.line_num = inputCount
                    error_rec.error = e
                    yield error_rec
                    
                if errorLimit and errorCount < errorLimit :
                    try :
                        self.counters['%sParseErrors'%format] += 1
                    except KeyError:
                        self.counters['%sParseErrors'%format] =1
                    continue
                else :
                    exp = 'Error limit exceeded while parsing. Last exception:\n%s'%e
                    raise ValueError(exp)

            if kFmt :
                key_rec = self.getRecord(kFmt)
                key_rec.fromString(key)
                key = key_rec
	            

            if returnRaw :
                yield (key, rec, value) if kV else (rec, value)
            else :
                yield (key, rec) if kV else (rec)


    def parseGroups(self, groups, **kwargs) :
        for key, records in groups :
            parsed_records = ( self.parseRecords(records, **kwargs) )
            yield(key, parsed_records)



    def loadLookup(self, lkp_file, lkp_format, key, **kwargs) :
        """
        load Lookup
        """
        compressed = kwargs.get('compressed', False)
        if compressed : 
            data = gzip.open(lkp_file)
        else :
            data = open(lkp_file)

        cache_dict = {}
        kv = kwargs.get('keyValue', False)
        kvsep = kwargs.get('keyValueSep', False)

        for line in data :
            line = line.strip('\r\n')
            line = line.decode('utf-8')
            rec = self.getRecord(lkp_format)

        if kv:
            k, s, value = line.partition(kvsep)
        else:
            value = line

        rec.fromString(value)
        cache_dict[rec.get(key)] = rec

        data.close()
        return cache_dict


    def leftJoin(self, formats) :
        pass


class SimpleParseError(SimpleRecordFormat) :
    def __init__(self) :
	    fields = ['line_num', 'error']
	    SimpleRecordFormat.__init__(self, fieldList=fields, name='SimpleParseError')


class join(object) :
    def __init__(self, inputs, **kwargs) :
       self.types = { 0: 'inner', 1: 'outer',
                      2: 'left',  3: 'right'}

       self.inputs = inputs
       self.join_type = kwargs.get('type', 'inner')

    def generate(self, stream) :
        pass

class ValidationError(Exception):
    def __init__(self, error_message):
        self.error_message = error_message
    def __str__(self):
        return self.error_message

class NotImplementedError(Exception):
    def __init__(self, error_message) :
        self.error_message = error_message
    def __str__(self):
        return self.error_message

class DefinitionError(Exception):
    def __init__(self, error_message) :
        self.error_message = error_message
    def __str__(self):
        return self.error_message

class ParseError(Exception): 
    def __init__(self, error_message) :
        self.error_message = error_message
    def __str__(self):
        return self.error_message


def parse_args() :
    parser = argparse.ArgumentParser(description='simple_record_format commandline interface')
    parser.add_argument('-d', '--dump', help='dumps a local data file using a simpleRecordFormat', required=False, destination='dump', action='store_true')
    parser.add_argument('-hd', '--hdfs_dump', help='dumps an hdfs data file using a simpleRecordFormat', required=False, destination='hdfs_dump', action='store_true')
    parser.add_argument('-f', '--file', help='record format file', required=False, destination='format_file')
    parser.add_argument('-r', '--format', help='format name in file', required=False, destination='format')



if __name__ == '__main__' :
    pass
