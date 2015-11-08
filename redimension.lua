local _USAGE = {
  'KEYS[1] - index sorted set key',
  'KEYS[2] - index hash key',
  'ARGV[1] - command. Can be:',
  '  create        - create an index with ARGV[2] as dimension and ARGV[3] as precision',
  '  drop          - drops an index',
  '  index         - index an element ARGV[2] with ARGV[3]..ARGV[3+dimension] values',
  '  unindex       - unindex an element ARGV[2] with ARGV[3]..ARGV[3+dimension] values',
  '  unindex_by_id - unindex an element by id ARGV[2]',
  '  update        - update an element ARGV[2] with ARGV[3]..ARGV[3+dimension] values',
  '  query         - query using ranges ARGV[2], ARGV[3]..ARGV[2+dimension-1], ARGV[2+dimension]',
  '  fuzzy_test    - fuzzily tests the library on ARGV[2] dimension with ARGV[3] items using ARGV[4] queries',
  }

local _dim  -- index's dimension
local _prec -- index's precision
local _MAX_PREC = 56

local bin2hex = {
    ['0000'] = '0',
    ['0001'] = '1',
    ['0010'] = '2',
    ['0011'] = '3',
    ['0100'] = '4',
    ['0101'] = '5',
    ['0110'] = '6',
    ['0111'] = '7',
    ['1000'] = '8',
    ['1001'] = '9',
    ['1010'] = 'A',
    ['1011'] = 'B',
    ['1100'] = 'C',
    ['1101'] = 'D',
    ['1110'] = 'E',
    ['1111'] = 'F'
}

local function load_meta()  
  _dim = tonumber(redis.call('HGET', KEYS[2], '_dim'))
  _prec = tonumber(redis.call('HGET', KEYS[2], '_prec'))
  if not _dim or not _prec then
    error('failed to load index meta data')
  end
end

local function check_dims(vars)
  if #vars ~= _dim then
    error('wrong number of values for this index')
  end
end

-- Encode N variables into the bits-interleaved representation.
local function encode(...)
  local comb = {}

  for i = 1, #arg do
    local b = arg[i]
    for j = 1, _prec do
      b = bit.rol(b, 1)
      if comb[j] then
        comb[j] = comb[j] .. bit.band(b, 1) 
      else
        table.insert(comb, bit.band(b, 1))
      end
    end
  end
  
  local bs = table.concat(comb)
  local l = string.len(bs)
  local rem = l % 4
  local hs = ''
  local b = ''
  
  l = l - 1
  if (rem > 0) then
    bs = string.rep('0', 4 - rem) .. bs
  end

  for i = 1, l, 4 do
    b = string.sub(bs, i, i+3)
    hs = hs .. bin2hex[b]
  end

  hs = string.rep('0', _prec*_dim/4-hs:len()) .. hs:sub(3):lower()
  return hs
end

-- Encode an element coordinates and ID as the whole string to add
-- into the sorted set.
local function elestring(vars, id)
  check_dims(vars)
  local ele = encode(unpack(vars))
  for _, v in pairs(vars) do
    ele = ele .. ':' .. v
  end
  ele = ele .. ':' .. id
  return ele
end
  
-- Add a variable with associated data 'id'
local function index(vars, id)
  local ele = elestring(vars, id)
  -- TODO: remove this debug helper
  if redis == nil then
    print(ele)
    return
  end
  redis.call('ZADD', KEYS[1], 0, ele)
  redis.call('HSET', KEYS[2], id, ele)
end

-- ZREM according to current position in the space and ID.
local function unindex(vars,id)
  redis.call('ZREM', KEYS[1], elestring(vars,id))
end

-- Unidex by just ID in case @hashkey is set to true in order to take
-- an associated Redis hash with ID -> current indexed representation,
-- so that the user can unindex easily.
local function unindex_by_id(id)
  local ele = redis.call('HGET', KEYS[2], id)
  redis.call('ZREM', KEYS[1], ele)
  redis.call('HDEL', KEYS[2], id)
end

-- Like index but makes sure to remove the old index for the specified
-- id. Requires hash mapping enabled.
local function update(vars,id)
  local ele = elestring(vars,id)
  local oldele = redis.call('HGET', KEYS[2], id)
  redis.call('ZREM', KEYS[1], oldele)
  redis.call('HDEL', KEYS[2], id)
  redis.call('ZADD', KEYS[1], 0, ele)
  redis.call('HSET', KEYS[2], id, ele)
end
  
--- exp is the exponent of two that gives the size of the squares
-- we use in the range query. N times the exponent is the number
-- of bits we unset and set to get the start and end points of the range.
local function query_raw(vrange,exp)
  local vstart = {}
  local vend = {}
  -- We start scaling our indexes in order to iterate all areas, so
  -- that to move between N-dimensional areas we can just increment
  -- vars.  
  for _, v in pairs(vrange) do
    table.insert(vstart, math.floor(v[1]/(2^exp)))
    table.insert(vend, math.floor(v[2]/(2^exp)))
  end

  -- Visit all the sub-areas to cover our N-dim search region.
  local ranges = {}
  local vcurrent = {}
  for i = 1, #vstart do
    table.insert(vcurrent, vstart[i])
  end
  
  local notdone = true
  while notdone do
    -- For each sub-region, encode all the start-end ranges
    -- for each dimension.
    local vrange_start = {}
    local vrange_end = {}
    for i = 1, _dim do
      table.insert(vrange_start, vcurrent[i]*(2^exp))
      table.insert(vrange_end, bit.bor(vrange_start[i],(2^exp)-1))
    end   
        
    -- Now we need to combine the ranges for each dimension
    -- into a single lexicographcial query, so we turn
    -- the ranges it into interleaved form.
    local s = encode(unpack(vrange_start))
    -- Now that we have the start of the range, calculate the end
    -- by replacing the specified number of bits from 0 to 1.
    local e = encode(unpack(vrange_end))
    table.insert(ranges, { '['..s, '['..e..':\255'  })

    -- Increment to loop in N dimensions in order to visit
    -- all the sub-areas representing the N dimensional area to
    -- query.
    for i = 1, _dim do
      if vcurrent[i] ~= vend[i] then
        vcurrent[i] = vcurrent[i] + 1
        break
      elseif i == _dim then
        notdone = false -- Visited everything!
      else
        vcurrent[i] = vstart[i]
      end
    end
  end
  
  -- Perform the ZRANGEBYLEX queries to collect the results from the
  -- defined ranges. Use pipelining to speedup.
  local allres = {}
  for _, v in pairs(ranges) do
    local res = redis.call('ZRANGEBYLEX', KEYS[1], v[1], v[2])
    for _, r in pairs(res) do
      table.insert(allres, r)
    end
  end

  -- Filter items according to the requested limits. This is needed
  -- since our sub-areas used to cover the whole search area are not
  -- perfectly aligned with boundaries, so we also retrieve elements
  -- outside the searched ranges.
  local items = {}
  for _, v in pairs(allres) do
    local fields = {}
    v:gsub('([^:]+)', function(f) table.insert(fields, f) end)
    local skip = false
    for i = 1, _dim do
      if tonumber(fields[i+1]) < vrange[i][1] or
        tonumber(fields[i+1]) > vrange[i][2]
      then
        skip = true
        break
      end
    end
    if not skip then
      table.remove(fields, 1)
      table.insert(items, fields)
    end
  end
  
  return items
end
    
-- Like query_raw, but before performing the query makes sure to order
-- parameters so that x0 < x1 and y0 < y1 and so forth.
-- Also calculates the exponent for the query_raw masking.
local function query(vrange)
  check_dims(vrange)
  local deltas = {}
  for i, v in ipairs(vrange) do
    if v[1] > v[2] then
      vrange[i][1], vrange[i][2] = vrange[i][2], vrange[i][1]
    end
    table.insert(deltas, vrange[i][2]-vrange[i][1]+1)
  end
  
  local delta = deltas[1]
  for _, v in pairs(deltas) do
    if v < delta then
      delta = v
    end
  end
  
  local exp = 1
  while delta > 2 do
    delta = math.floor(delta / 2)
    exp = exp + 1
  end
  
  -- If ranges for different dimensions are extremely different in span,
  -- we may end with a too small exponent which will result in a very
  -- big number of queries in order to be very selective. This is most
  -- of the times not a good idea, so at the cost of querying larger
  -- areas and filtering more, we scale 'exp' until we can serve this
  -- request with less than 20 ZRANGEBYLEX commands.
  --
  -- Note: the magic "20" depends on the number of items inside the
  -- requested range, since it's a tradeoff with filtering items outside
  -- the searched area. It is possible to improve the algorithm by using
  -- ZLEXCOUNT to get the number of items.
  while true do
    for i, v in ipairs(vrange) do
      deltas[i] = (v[2]/(2^exp))-(v[1]/(2^exp))+1
    end
    local ranges = 1
    for _, v in pairs(deltas) do
      ranges = ranges*v
    end
    
    if ranges < 20 then
      break
    end
    exp = exp + 1
  end
    
  return query_raw(vrange,exp)
end

-- Similar to query but takes just the center of the query area and a
-- radius, and automatically filters away all the elements outside the
-- specified circular area.
local function query_radius(x,y,exp,radius)
  -- TODO
end

-- drops an index
local function drop()
  redis.call('DEL', KEYS[1], KEYS[2])
end

-- creates an index with dimension d and precision p
local function create(d, p)
  drop()
  redis.call('HMSET', KEYS[2], '_dim', d, '_prec', p)
end

-- parse arguments
if #ARGV == 0 or #KEYS ~= 2 then
  return(_commands)
end

local cmd = ARGV[1]:lower()

if cmd == 'create' then
  local dim, prec = tonumber(ARGV[2]), tonumber(ARGV[3])
  if dim == nil or prec == nil then
    error('index dimension and precision are must be numbers')
  end
  if dim < 1 then
    error('index dimension has to be at least 1')
  end
  if prec < 1 or prec > _MAX_PREC then
    error('index precision has to be between 1 and ' .. _MAX_PREC)
  end
  create(dim, prec)
  return({dim, prec})
end

if cmd == 'drop' then
  drop()
  return('dropped.')
end

-- not really fuzzy w/o changing replication mode and using real randoms
if cmd == 'fuzzy_test' then
  local dim, items, queries = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
  local timings = {}
  local avgt = 0.0
  
  drop()
  create(dim, _MAX_PREC)
  load_meta()
  
  local id = 0
  local dataset = {}
  for i = 1, items do
    local vars = {}
    for j = 1, dim do
      table.insert(vars, math.random(1000))
    end
    index(vars, id)
    table.insert(vars, id)
    table.insert(dataset, vars)
    id = id + 1
  end

  for i = 1, queries do
    local random = {}
    for j = 1, dim do
      local s = math.random(1000)
      local e = math.random(1000)
      if e > s then
        s, e = e, s
      end
      table.insert(random, { s, e })
    end
    
    local start_t = redis.call('TIME')
    local res1 = query(random)
    local end_t = redis.call('TIME')
    
    start_t[1], start_t[2] = tonumber(start_t[1]), tonumber(start_t[2])
    end_t[1], end_t[2] = tonumber(end_t[1]), tonumber(end_t[2])
    if end_t[2] > start_t[2] then
      table.insert(timings, { end_t[1] - start_t[1], end_t[2] - start_t[2] })
    else
      table.insert(timings, { end_t[1] - start_t[1] - 1, math.abs(end_t[2] - start_t[2]) })
    end
    
    avgt = (avgt * (#timings - 1) + tonumber(string.format('%d.%06d', timings[#timings][1], timings[#timings][2]))) / #timings
    
    local res2 = {}
    for _, v in pairs(dataset) do
      local included = true
      for j = 1, dim do
        if v[j] < random[j][1] or v[j] > random[j][2] then
          included = false
        end
      end
      if included then
        table.insert(res2, v)
      end
    end
    
    if #res1 ~= #res2 then
      return {{'dataset', dataset}, {'random', random}, {'res1', res1}, {'res2', res2}}
      -- error('ERROR ' .. #res1 .. ' VS ' .. #res2)
    end
    
    -- table sorting is so much FUN!
    local function cmp(a, b, depth)
      depth = depth or 1
      
      if a[depth] < b[depth] then
        return true
      end
      if a[depth] == b[depth] then
        return depth == dim or cmp(a, b, depth + 1)
      end
      if a[depth] > b[depth] then
        return false
      end
      table.sort(res1, cmp)
      table.sort(res2, cmp)
      
      for i1, r1 in ipairs(res1) do
        for i2 = 1, dim + 1 do
          if r1[i2] ~= res2[i1][i2] then
            error('ERROR ' .. r1[i2] .. ' ~= ' .. res2[i1][i2])
          end
        end
      end
    end
  end

  -- housekeeping drop() can't be called unless replication mode is changed
  return({ 'fuzzily tested.', {dim, items, queries, 'avg query time (sec): ' .. tostring(avgt)}})
end

load_meta()

if cmd == 'index'  or cmd == 'unindex' or cmd == 'update' then
  local id = ARGV[2]
  local vars = {}
  for i = 3,#ARGV do
    table.insert(vars, tonumber(ARGV[i]))
  end
  
  if cmd == 'index' then
    index(vars, id)
    return('indexed.')
  elseif cmd == 'unindex' then
    unindex(vars, id)
    return('unindexed.')
  else
    update(vars, id)
    return('updated.')
  end
end

if cmd == 'unindex_by_id' then
  local id = ARGV[2]
  
  unindex_by_id(id)
  return('unindexed by id.')
end

if cmd == 'query' then
  local vranges = {}
  for i = 1, _dim do
    table.insert(vranges, {tonumber(ARGV[i*2]), tonumber(ARGV[i*2+1])})
  end

  return query(vranges)
end

return(_USAGE)