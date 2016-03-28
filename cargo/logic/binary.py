"""
  `Binary Logic and Operations`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import psycopg2.extensions
from cargo.expressions import *


__all__ = ('BinaryLogic',)


class BinaryLogic(BaseLogic):
    __slots__ = tuple()
    CONCAT_OP = '||'

    def _cast_bytes(self, string):
        if isinstance(string, bytes):
            return psycopg2.extensions.Binary(string)
        return string

    def concat(self, string, **kwargs):
        """ String concatenation
            -> (:class:Expression)
        """
        string = self._cast_bytes(string)
        return Expression(self, self.CONCAT_OP, string, **kwargs)

    def octet_length(self, **kwargs):
        """ Number of bytes in binary string
            -> (:class:Function)
        """
        return Function('octet_length', self, **kwargs)

    def overlay(self, substring, from_, for_=None, **kwargs):
        """ Replace @substring
            -> (:class:Function)
        """
        substring = self._cast_bytes(substring)
        exps = [self,
                Expression(self.empty,
                           'placing',
                           Expression(substring, 'from', from_))]
        if for_:
            exps.append(Expression(self.empty, 'for', for_))
        return Function('overlay', Clause("", *exps), **kwargs)

    def position(self, substring):
        """ Location of specified @substring
            -> (:class:Function)
        """
        substring = self._cast_bytes(substring)
        return Function('position', Expression(substring, 'in', self))

    def substring(self, from_=None, for_=None, **kwargs):
        """ Extracts substring from @from_ to @for_
            -> (:class:Function)
        """
        exps = []
        if from_ is not None:
            exps.append(Expression(self.empty, 'from', from_))
        if for_ is not None:
            exps.append(Expression(self.empty, 'for', for_))
        return Function('substring', Clause("", self, *exps), **kwargs)

    def trim(self, bytes_, both=False, **kwargs):
        """ Remove the longest string containing only the bytes in @bytes_
            from the start and end of the string
            -> (:class:Expression)
        """
        bytes_ = self._cast_bytes(bytes_)
        exp = Expression(bytes_, 'from', self)
        if both:
            exp = Clause('both', exp)
        return Function('trim', exp, **kwargs)

    def encode(self, format, **kwargs):
        """ Encode binary data into a textual representation. Supported
            formats are: base64, hex, escape. escape converts zero bytes and
            high-bit-set bytes to octal sequences (\nnn) and doubles
            backslashes.
            -> (:class:Function)
        """
        return F.encode(self, format, **kwargs)

    def decode(self, format, **kwargs):
        """ Decode binary data from textual representation in string. Options
            for format are same as in encode.
            -> (:class:Function)
        """
        return F.decode(self, format, **kwargs)

    def get_bit(self, offset, **kwargs):
        """ Extract bit from @string
            -> (:class:Function)
        """
        return F.get_bit(self, offset, **kwargs)

    def get_byte(self, offset, **kwargs):
        """ Extract byte from @string
            -> (:class:Function)
        """
        return F.get_byte(self, offset, **kwargs)

    def set_bit(self, offset, new_value, **kwargs):
        """ Set bit in @string
            -> (:class:Function)
        """
        return F.set_bit(self, offset, new_value, **kwargs)

    def set_byte(self, offset, new_value, **kwargs):
        """ Set byte in @string
            -> (:class:Function)
        """
        return F.set_byte(self, offset, new_value, **kwargs)

    def length(self, **kwargs):
        """ Length of binary @string
            -> (:class:Function)
        """
        return F.length(self, **kwargs)

    def md5(self, **kwargs):
        """ Calculates the MD5 hash of @string, returning the result in
            hexadecimal.
            -> (:class:Function)
        """
        return F.md5(self, **kwargs)
