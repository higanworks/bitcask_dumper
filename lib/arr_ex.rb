## pickup Link from bitcask values

class Array
 def _find_array(key, stack)
   self.each do |item|
     if item.is_a?(Array)
       item._find_array(item, stack)
     elsif item == "Links"
       stack << self
     end
   end
 end

 def find_array(key)
   ret = []
   _find_array(key, ret)
   ret
 end
end

