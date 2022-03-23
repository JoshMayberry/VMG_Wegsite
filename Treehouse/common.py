""" These functions are generic and not project specific. """

import typing
import inspect
import itertools

class Singleton():
	"""Used to get values correctly.

	Example Use: FLAG = MyUtilities.common.Singleton("FLAG")
	"""

	def __init__(self, label = "Singleton", *, state = None, private = False):
		"""
		label (str) - What this singleton is called
		private (bool) - Determines if only this module may use this singleton (Not Implemented)
		state (bool) - Determiens what happens if this singleton is evaluated by bool()
			- If True: Will always return True
			- If False: Will always return False
			- If None: No special action is taken
		"""

		self.private = private

		if (not self.private):
			self.label = label
		else:
			self.label = f"{label} ({__name__} only)"

		if (state is not None):
			if (state):
				self.__bool__ = lambda self: True
			else:
				self.__bool__ = lambda self: False

	def __repr__(self):
		return f"{self.label}()"

NULL = Singleton("NULL", state = False)
NULL_private = Singleton("NULL", state = False, private = True)

class ELEMENT():
	"""Used to make a class pass ensure_container() as an element instead of a container."""

class CATALOGUE():
	"""Used to make a class pass ensure_dict() as a dict instead of a dict key or value."""

def oneOrMany(answer, *, forceTuple = False, forceContainer = True, 
	consumeFunction = True, isDict = False, returnForNone = None):
	"""Returns the the first element of the list if it is one item long.
	Otherwise, returns the list.

	Example Input: oneOrMany(answer)
	Example Input: oneOrMany(yieldAnswer)
	Example Input: oneOrMany(yieldAnswer())
	Example Input: oneOrMany(answer, forceTuple = True)
	Example Input: oneOrMany(myList, forceContainer = False)
	Example Input: oneOrMany(myClass, consumeFunction = False)
	Example Input: oneOrMany(catalogue, isDict = True, forceContainer = False)
	"""

	if (consumeFunction and (inspect.ismethod(answer) or inspect.isfunction(answer))):
		answer = answer()

	if (forceContainer and (not isDict)):
		answer = ensure_container(answer)

	if (forceTuple or (len(answer) != 1)):
		return answer
	elif (isDict):
		return next(iter(answer.values()), returnForNone)
	else:
		return next(iter(answer), returnForNone)

def is_container(item, *, elementTypes = None, elementCriteria = None):
	"""Returns if the given item is a container or not.
	Generators are not considered containers.

	elementTypes (list) - Extra types that are ok to be elements
	elementCriteria (tuple) - Allows for formatted tuples to pass as elements if they match the criteria
		~ (required length (int), required type (type))
		- If None: Will count all tuples as containers

	Example Input: is_container(valueList)
	Example Input: is_container(valueList, evaluateGenerator = False)
	Example Input: is_container(handle, elementTypes = (Base,))
	Example Input: is_container((255, 255, 0), elementCriteria = (3, int))
	Example Input: is_container((255, 255, 0), elementCriteria = ((3, int), (4, int)))
	Example Input: is_container(("lorem", 1), elementCriteria = (2, (str, int)))
	"""

	def checkItem(_item, _type):
		if (_type is None):
			return True

		return isinstance(_item, _type)

	def checkType(requiredLength, requiredType):
		nonlocal item

		if (len(item) != requiredLength):
			return False

		if (requiredType is None):
			return True

		container = ensure_container(requiredType)
		return all(checkItem(*_item) for _item in itertools.zip_longest(item, container, fillvalue = ensure_lastElement(container)))

	###########################

	if (isinstance(item, (str, ELEMENT, typing.Mapping, typing.MutableMapping))):
		return False

	if (not isinstance(item, typing.Iterable)):
		return False

	if (isinstance(item, tuple(ensure_container(elementTypes, convertNone = True)))):
		return False

	if ((elementCriteria is not None) and isinstance(item, (tuple, list))):
		if (not item):
			return True

		if (not isinstance(elementCriteria[0], tuple)): ## TO DO ## This line is clunky; find another way
			elementCriteria = (elementCriteria,)

		return not any(checkType(*required) for required in elementCriteria)

	if (isinstance(item, (list, tuple, set))):
		return True

def ensure_container(item, *args, useForNone=None, convertNone = True, is_container_answer = NULL_private, 
	returnForNone=None, evaluateGenerator=True, consumeFunction = True, **kwargs):
	"""Makes sure the given item is a container.
	Iterable objects are not counted as containers if they inherit ELEMENT.

	args (*) - What should be appended to the end of the container 'item'
	returnForNone (any) - What should be returned if 'item' is None
		- If function: will return whatever the function returns

	Example Input: ensure_container(valueList)
	Example Input: ensure_container(valueList, convertNone = False)
	Example Input: ensure_container(valueList, evaluateGenerator = False)
	Example Input: ensure_container((x for x in range(3)))
	Example Input: ensure_container(handle, elementTypes = (Base,))
	Example Input: ensure_container((255, 255, 0), elementCriteria = (3, int))
	Example Input: ensure_container((255, 255, 0), elementCriteria = ((3, int), (4, int)))
	Example Input: ensure_container(valueList, convertNone=False, returnForNone=lamda:[1,2,3])
	"""

	if (args):
		return (*ensure_container(item, useForNone = useForNone, convertNone = convertNone, is_container_answer = is_container_answer, 
			returnForNone = returnForNone, evaluateGenerator = evaluateGenerator, consumeFunction = consumeFunction, **kwargs), *args)

	if (item is useForNone):
		if (convertNone):
			return ()
		if (consumeFunction and (inspect.ismethod(returnForNone) or inspect.isfunction(returnForNone))):
			return returnForNone()
		return (returnForNone,)

	if (is_container_answer is NULL_private):
		state = is_container(item, **kwargs)
	else:
		state = is_container_answer

	if (state):
		return item
	if (state is not None):
		return (item,)

	if (evaluateGenerator and isinstance(item, typing.Iterable)):
		return ensure_container(tuple(item), useForNone = useForNone, convertNone = convertNone, returnForNone = returnForNone, evaluateGenerator = evaluateGenerator, **kwargs)
	return (item,)

def ensure_dict(catalogue, defaultKey=None, *, useAsKey=False, convertNone=True, useForNone=None, returnForNone=NULL_private):
	"""Makes sure the given catalogue is a dictionary.
	Objects are counted as dictionaries if they inherit CATALOGUE.

	catalogue (any) - What to make sure is a dictionary
	defaultKey (any) - What to use for the key if *catalogue* is not a dictionary
	useAsKey (bool) - Determines how *catalogue* is used if it is not a dictionary
		- If True: {catalogue: defaultKey}
		- If False: {defaultKey: catalogue}
		- If None: {catalogue: catalogue}
	convertNone (bool) - If when *catalogue* is None, return something special
	useForNone (any) - What to compare against to see if it is None
	returnForNone (any) - What to return if *catalogue* is None; defaults to an empty dictionary

	Example Input: ensure_dict("value")
	Example Input: ensure_dict("value", "key")
	Example Input: ensure_dict({"key": "value"})
	Example Input: ensure_dict("key", "value", useAsKey=True)
	Example Input: ensure_dict(None, useForNone=common.NULL)
	Example Input: ensure_dict(None, "key", convertNone=False)
	"""

	if (convertNone and (catalogue is useForNone)):
		if (returnForNone is NULL_private):
			return {}
		return returnForNone

	elif (isinstance(catalogue, (dict, CATALOGUE))):
		return catalogue

	if (useAsKey is None):
		return {catalogue: catalogue}
	if (useAsKey):
		return {catalogue: defaultKey}
	return {defaultKey: catalogue}
