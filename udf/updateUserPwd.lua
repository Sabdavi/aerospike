function updatePassword(topRec,pwd)
   -- Assign new password to the user record
   topRec['password'] = pwd
   -- Update user record
   aerospike:update(topRec)
   -- return new password
   return topRec['password']
end