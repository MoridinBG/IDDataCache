//
//  IDDataCache.swift
//  IDDataCache
//
//  Created by Ivan Dilchovski on 5/29/15.
//  Copyright (c) 2015 TechLight. All rights reserved.
//

import Foundation
import CommonCrypto

public enum IDDataCacheType
{
    case None
    case Disk
    case Memory
}

/**
* IDDataCache maintains a memory cache and an optional disk cache. Disk cache write operations are performed
* asynchronous so it doesn’t add unnecessary latency to the UI.
*/
public class IDDataCache
{
    // MARK: - Properties

    // MARK: Private
    
    /**
    * Private properties
    */
    private let kDefaultCacheMaxCacheAge = 60 * 60 * 24 * 7
    private var memCache: NSCache
    private var diskCachePath: String = "" //This is needed so Xcode wouldn't complain about makeDiskCachePath being initialized before all properties
    private var customPaths: Set<String>
    private var fileManager: NSFileManager
    private var ioQueue: dispatch_queue_t
    
    static private var sharedNamedInstances = [String : IDDataCache]()
    static private var sharedNamedPersistentInstances = [String : IDDataCache]()
    
    // MARK: Public
    
    /**
    * The maximum "total cost" of the in-memory data cache. The cost function is the number of bytes held in memory.
    */
    public var maxMemoryCost: Int
    
    /**
    * The maximum number of objects the cache should hold.
    */
    public var maxMemoryCountLimit: Int
    
    /**
    * The maximum length of time to keep a data object in the cache, in seconds
    */
    public var maxCacheAge: Int
    
    /**
    * The maximum size of the cache, in bytes.
    */
    public var maxCacheSize: Int
    
    /**
    * If the cache should persist the files in Documents folder
    */
    public var isPersistent: Bool
    
    /**
    * Returns global shared cache instance
    *
    * @return IDDataCache global instance
    */
    static public let sharedInstance = IDDataCache()
    
    /**
    * Returns the names of all the named Persistent caches in use
    *
    * @return [IDDataCache] all global persistent instances
    */
    static public var sharedPersistentInstanceNames: [String]
    {
        get
        {
            return sharedNamedPersistentInstances.keys.array
        }
    }
    
    // MARK: - Methods
    
    /**
    * Returns global shared named cache instance
    *
    * @return IDDataCache global named instance
    */
    class  public func sharedNamedInstance(namespace: String) -> IDDataCache
    {
        if let instance = sharedNamedInstances[namespace]
        {
            return instance
        } else
        {
            let instance = IDDataCache(namespace: namespace)
            sharedNamedInstances[namespace] = instance
            
            return instance
        }
    }
    
    /**
    * Returns global shared named persistent cache instance
    *
    * @return IDDataCache global named persistent instance
    */
    class  public func sharedNamedPersistentInstance(namespace: String) -> IDDataCache
    {
        if let instance = sharedNamedPersistentInstances[namespace]
        {
            return instance
        } else
        {
            let instance = IDDataCache(namespace: namespace, isPersistent: true)
            sharedNamedPersistentInstances[namespace] = instance
            
            return instance
        }
    }
    
    public convenience init()
    {
        self.init(namespace: "default")
    }
    
    /**
    * Init a new cache store with a specific namespace
    *
    * @param namespace The namespace to use for this cache store
    */
    public convenience init(namespace: String)
    {
        self.init(namespace: namespace, isPersistent: false)
    }
    
    public init(namespace: String, isPersistent: Bool)
    {
        let fullNamespace = "com.techlight.IDDataCache." + namespace
        self.memCache = NSCache()
        self.memCache.name = fullNamespace
        
        self.maxMemoryCost = 0
        self.maxMemoryCountLimit = 0
        self.maxCacheAge = kDefaultCacheMaxCacheAge
        self.maxCacheSize = 0
        self.isPersistent = isPersistent
        self.customPaths = Set<String>()
        self.ioQueue = dispatch_queue_create("com.techlight.IDDataCache", DISPATCH_QUEUE_SERIAL)
        self.fileManager = NSFileManager()
        self.diskCachePath = makeDiskCachePath(fullNamespace)
        dispatch_async(ioQueue) {
            self.fileManager = NSFileManager()
        }
        
    }
    
    public func makeDiskCachePath(fullNamespace: String) -> String
    {
        let paths: String
        if !self.isPersistent
        {
            paths = NSSearchPathForDirectoriesInDomains(.CachesDirectory, .UserDomainMask, true)[0]
        } else
        {
            paths = NSSearchPathForDirectoriesInDomains(.DocumentDirectory, .UserDomainMask, true)[0]
        }

        return paths.stringByAppendingPathComponent(fullNamespace)
    }
    
    /**
    * Add a read-only cache path to search for images pre-cached by IDDataCache
    * Useful if you want to bundle pre-loaded data objects with your app
    *
    * @param path The path to use for this read-only cache path
    */
    public func addReadOnlyCachePath(path: String)
    {
        customPaths.insert(path)
    }
    
    /**
    *  Get the cache path for a certain key (needs the cache path root folder)
    *
    *  @param key  the key (can be obtained from url using cacheKeyForURL)
    *  @param path the cach path root folder
    *
    *  @return the cache path
    */
    public func cachePathForKey(key: String, path: String) -> String
    {
        let filename = cachedFileNameForKey(key)
        return path.stringByAppendingPathComponent(filename)
    }
    
    /**
    *  Get the default cache path for a certain key
    *
    *  @param key the key (can be obtained from url using cacheKeyForURL)
    *
    *  @return the default cache path
    */
    public func defaultCachePathForKey(key: String) -> String
    {
        return cachePathForKey(key, path: self.diskCachePath)
    }
    
    /**
    * Store a daa object into memory and optionally disk cache at the given key.
    *
    * @param data   The data to store
    * @param key    The unique data cache key, usually it's data absolute URL
    * @param toDisk Store the data to disk cache if YES
    */
    public func storeData(data: NSData, forKey key:String, toDisk: Bool)
    {
        let cost = IDCacheCostForData(data)
        self.memCache.setObject(data, forKey: key, cost: cost)
        
        if (toDisk)
        {
            dispatch_async(ioQueue) {
                if !self.fileManager.fileExistsAtPath(self.diskCachePath)
                {
                    do
					{
                        try self.fileManager.createDirectoryAtPath(self.diskCachePath, withIntermediateDirectories: false, attributes: nil)
                    } catch _
					{
                    }
                }
                
                self.fileManager.createFileAtPath(self.defaultCachePathForKey(key), contents: data, attributes: nil)
            }
        }
    }
    
    /**
    * Store a data object into memory and disk cache at the given key.
    *
    * @param data  The data to store
    * @param key   The unique data cache key, usually it's data absolute URL
    */
    public func storeData(data: NSData, forKey key:String)
    {
        storeData(data, forKey: key, toDisk: true)
    }
    
    /**
    * Query the disk cache asynchronously.
    *
    * @param key The unique key used to store the wanted data object
    */
    public func queryDiskCacheForKey(key: String, done: (data: NSData, cacheType: IDDataCacheType) -> Void) -> NSOperation?
    {
        // First check the in-memory cache...
        if let data = dataFromMemoryCacheForKey(key)
        {
            done(data: data, cacheType: .Memory)
            return nil
        }
        
        let operation = NSOperation()
        dispatch_async(ioQueue) {
            if operation.cancelled
            {
                return
            }
            
            if let diskData = self.diskImageForKey(key)
            {
                let cost = self.IDCacheCostForData(diskData)
                self.memCache.setObject(diskData, forKey: key, cost: cost)
                
                dispatch_async(dispatch_get_main_queue()) {
                    done(data: diskData, cacheType: .Disk)
                }
            }
            
        }
        
        return operation
    }


    /**
    * Query the memory cache synchronously.
    *
    * @param key The unique key used to store the wanted data object
    */
    public func dataFromMemoryCacheForKey(key: String) -> NSData?
    {
        return memCache.objectForKey(key) as? NSData
    }
    
    /**
    * Query the disk cache synchronously after checking the memory cache.
    *
    * @param key The unique key used to store the wanted data object
    */
    public func dataFromDiskCacheForKey(key: String) -> NSData?
    {
        // First check the in-memory cache...
        if let data = dataFromMemoryCacheForKey(key)
        {
            return data
        }
        
        // Second check the disk cache...
        if let diskData = diskImageForKey(key)
        {
            let cost = IDCacheCostForData(diskData)
            memCache.setObject(diskData, forKey: key, cost: cost)
            return diskData
        }
        
        return nil
    }
    
    /**
    * Remove the data object from memory and disk cache synchronously
    *
    * @param key The unique data object cache key
    */
    public func removeDataForKey(key: String)
    {
        removeDataForKey(key, completion: nil)
    }
    
    
    /**
    * Remove the data object from memory and disk cache synchronously
    *
    * @param key             The unique data object cache key
    * @param completion      An block that should be executed after the data object has been removed (optional)
    */
    public func removeDataForKey(key: String, completion: (() -> Void)?)
    {
        removeDataForKey(key, fromDisk: true, completion: completion)
    }
    
    /**
    * Remove the data object from memory and optionally disk cache synchronously
    *
    * @param key      The unique data object cache key
    * @param fromDisk Also remove cache entry from disk if YES
    */
    public func removeDataForKey(key: String, fromDisk: Bool)
    {
        removeDataForKey(key, fromDisk: fromDisk, completion: nil)
    }
    
    /**
    * Remove the data object from memory and optionally disk cache synchronously
    *
    * @param key             The unique data object cache key
    * @param fromDisk        Also remove cache entry from disk if YES
    * @param completion      An block that should be executed after the data object has been removed (optional)
    */
    public func removeDataForKey(key: String, fromDisk: Bool, completion: (() -> Void)?)
    {
        memCache.removeObjectForKey(key)
        
        if fromDisk
        {
            dispatch_async(ioQueue) {
                do
				{
                    try self.fileManager.removeItemAtPath(self.defaultCachePathForKey(key))
                } catch _
				{
                }
                
                if let completion = completion
                {
                    dispatch_async(dispatch_get_main_queue()) {
                        completion()
                    }
                }
            }
        } else if let completion = completion
        {
            completion()
        }
    }
    
    /**
    * Clear all memory cached data objects
    */
    public func clearMemory()
    {
        memCache.removeAllObjects()
    }
    
    /**
    * Clear all disk cached data objects. Non-blocking method - returns immediately.
    * @param completion    An block that should be executed after cache expiration completes (optional)
    */
    public func clearDiskWithCompletion(completion: (() -> Void)?)
    {
        dispatch_async(ioQueue) {
            do
			{
                try self.fileManager.removeItemAtPath(self.diskCachePath)
            } catch _
			{
            }
			
            if(!self.isPersistent)
            {
                do
				{
                    try self.fileManager.createDirectoryAtPath(self.diskCachePath, withIntermediateDirectories: true, attributes: nil)
                } catch _
				{
                }
            }
            
            if let completion = completion
            {
                dispatch_async(dispatch_get_main_queue()) {
                    completion()
                }
            }
        }
    }
    
    /**
    * Clear all disk cached data objects
    * @see clearDiskOnCompletion:
    */
    public func clearDisk()
    {
        clearDiskWithCompletion(nil)
    }
    
    /**
    * Remove all expired cached data objects from disk. Non-blocking method - returns immediately.
    * @param completionBlock An block that should be executed after cache expiration completes (optional)
    */
    public func cleanDiskWithCompletionBlock(completion: (() -> Void)?)
    {
        dispatch_async(ioQueue) {
            let diskCacheURL = NSURL(fileURLWithPath: self.diskCachePath, isDirectory: true)
            let resourceKeys = [NSURLIsDirectoryKey, NSURLContentModificationDateKey, NSURLTotalFileAllocatedSizeKey]
            
            // This enumerator prefetches useful properties for our cache files.
            let fileEnumerator = self.fileManager.enumeratorAtURL(diskCacheURL, includingPropertiesForKeys: resourceKeys, options: .SkipsHiddenFiles, errorHandler: nil)!
            
            let expirationDate = NSDate(timeIntervalSinceNow: NSTimeInterval(-self.maxCacheAge))
            var cacheFiles = [NSURL : [NSString:AnyObject]]()
            var currentCacheSize = 0
            
            // Enumerate all of the files in the cache directory.  This loop has two purposes:
            //
            //  1. Removing files that are older than the expiration date.
            //  2. Storing file attributes for the size-based cleanup pass.
            var urlsToDelete = [NSURL]()
            for fileURL in fileEnumerator
            {
                let resourceValues = fileURL.resourceValuesForKeys(resourceKeys, error: nil) as! [NSString:AnyObject]
                
                // Skip directories.
                // TODO: May fail
                let isDirectory = resourceValues[NSURLIsDirectoryKey] as! Bool
                if  isDirectory == true
                {
                    continue
                }
                
                // Remove files that are older than the expiration date if there is max cache age set
                if self.maxCacheAge > 0
                {
                    let modificationDate = resourceValues[NSURLContentModificationDateKey] as! NSDate
                    if modificationDate.laterDate(expirationDate).isEqualToDate(expirationDate)
                    {
                        urlsToDelete.append(fileURL as! NSURL)
                        continue
                    }
                }
                
                // Store a reference to this file and account for its total size.
                let totalAllocatedSize = resourceValues[NSURLTotalFileAllocatedSizeKey] as! Int
                currentCacheSize += totalAllocatedSize
                cacheFiles[fileURL as! NSURL] = resourceValues
            }
            
            for fileURL in urlsToDelete
            {
                do
				{
                    try self.fileManager.removeItemAtURL(fileURL)
                } catch _
				{
                }
            }
            
            // If our remaining disk cache exceeds a configured maximum size, perform a second
            // size-based cleanup pass.  We delete the oldest files first.
            if self.maxCacheSize > 0 && currentCacheSize > self.maxCacheSize
            {
                // Target half of our maximum cache size for this cleanup pass.
                let desiredCacheSize = self.maxCacheSize / 2
                
                // Sort the remaining cache files by their last modification time (oldest first).
                var sortedFiles = [NSURL]()
                let sortedKeyValues =  Array(cacheFiles).sort { (keyValue1, keyValue2) -> Bool in
                    let date1 = keyValue1.1[NSURLContentModificationDateKey] as! NSDate
                    let date2 = keyValue2.1[NSURLContentModificationDateKey] as! NSDate
                    
                    return date1.timeIntervalSinceReferenceDate > date2.timeIntervalSinceReferenceDate
                }
				
                for (key, value) in sortedKeyValues
                {
                    sortedFiles.append(key)
                }
                
                
                for fileURL in sortedFiles
                {
                    do {
                        try self.fileManager.removeItemAtURL(fileURL)
                        let resourceValues = cacheFiles[fileURL]!
                        let totalAllocatedSize = resourceValues[NSURLTotalFileAllocatedSizeKey] as! Int
                        currentCacheSize -= totalAllocatedSize
                        
                        if (currentCacheSize < desiredCacheSize)
                        {
                            break
                        }
                    } catch _ {
                    }
                }
            }
            
            if let completion = completion
            {
                dispatch_async(dispatch_get_main_queue()) {
                    completion()
                }
            }
        }
    }
    
    /**
    * Remove all expired cached data objects from disk
    * @see cleanDiskWithCompletionBlock:
    */
    public func cleanDisk()
    {
        cleanDiskWithCompletionBlock(nil)
    }
    
    /**
    * Get the size used by the disk cache
    */
    func getSize() -> Int
    {
        var size = 0
        dispatch_sync(ioQueue) {
            let fileEnumerator = self.fileManager.enumeratorAtPath(self.diskCachePath)!
            for fn in fileEnumerator
            {
                let filename = fn as! String
                let filePath = self.diskCachePath.stringByAppendingPathComponent(filename)
                let attrs = try! NSFileManager.defaultManager().attributesOfItemAtPath(filePath)
                size += attrs[NSFileSize] as! Int
            }
        }
        
        return size
    }
    
    /**
    * Get the number of data objects in the disk cache
    */
    public func getDiskCount() -> Int
    {
        var count = 0
        dispatch_sync(ioQueue) {
            let fileEnumerator = self.fileManager.enumeratorAtPath(self.diskCachePath)!
            count = fileEnumerator.allObjects.count
        }
        
        return count
    }
    
    /**
    * Asynchronously calculate the disk cache's size.
    */
    public func calculateSizeWithCompletionBlock(completion: (fileCount: Int, totalSize: Int) -> Void)
    {
        let diskCacheURL = NSURL(fileURLWithPath: diskCachePath, isDirectory: true)
        dispatch_async(ioQueue) {
            var fileCount = 0
            var totalSize = 0
            
            let fileEnumerator = self.fileManager.enumeratorAtURL(diskCacheURL, includingPropertiesForKeys: [NSFileSize], options: .SkipsHiddenFiles, errorHandler: nil)!
            for fileURL in fileEnumerator
            {
                var fileSize: AnyObject?
                let url = fileURL as! NSURL
                do
				{
                    try url.getResourceValue(&fileSize, forKey: NSURLFileSizeKey)
                } catch _
				{
                }
                if let size = fileSize as? Int
                {
                    totalSize += size
                }
                fileCount += 1
            }
            dispatch_async(dispatch_get_main_queue()) {
                completion(fileCount: fileCount, totalSize: totalSize)
            }
        }
    }
    
    /**
    *  Check if data object exists in disk cache already (does not load the data object)
    *
    *  @param key the key describing the url
    *
    *  @return YES if an data object exists for the given key
    */
    public func diskDataExistsWithKey(key: String) -> Bool
    {
        // this is an exception to access the filemanager on another queue than ioQueue, but we are using the shared instance
        // from apple docs on NSFileManager: The methods of the shared NSFileManager object can be called from multiple threads safely.
        return NSFileManager.defaultManager().fileExistsAtPath(defaultCachePathForKey(key))
    }
    
    /**
    *  Async check if data object exists in disk cache already (does not load the data object)
    *
    *  @param key             the key describing the url
    *  @param completionBlock the block to be executed when the check is done.
    *  @note the completion block will be always executed on the main queue
    */
    public func diskDataExistsWithKey(key: String, completion: (isInCache: Bool) -> Void)
    {
        dispatch_async(ioQueue) {
            let exists = self.fileManager.fileExistsAtPath(self.defaultCachePathForKey(key))
            dispatch_async(dispatch_get_main_queue()) {
                completion(isInCache: exists)
            }
        }
    }
    
    // MARK: - Private methods
    
    private func backgroundCleanDisk()
    {
        //TODO: Implement me
        print("backgroundCleanDisk() is Stub")
//        Class UIApplicationClass = NSClassFromString(@"UIApplication");
//        if(!UIApplicationClass || ![UIApplicationClass respondsToSelector:@selector(sharedApplication)]) {
//            return;
//        }
//        UIApplication *application = [UIApplication performSelector:@selector(sharedApplication)];
//        __block UIBackgroundTaskIdentifier bgTask = [application beginBackgroundTaskWithExpirationHandler:^{
//        // Clean up any unfinished task business by marking where you
//        // stopped or ending the task outright.
//        [application endBackgroundTask:bgTask];
//        bgTask = UIBackgroundTaskInvalid;
//        }];
//        
//        // Start the long-running task and return immediately.
//        [self cleanDiskWithCompletionBlock:^{
//        [application endBackgroundTask:bgTask];
//        bgTask = UIBackgroundTaskInvalid;
//        }];
    }
    
    private func diskImageDataBySearchingAllPathsForKey(key: String) -> NSData?
    {
        let defaultPath = defaultCachePathForKey(key)
        if let data = NSData(contentsOfFile: defaultPath)
        {
            return data
        }
        
        for path in customPaths
        {
            let filePath = cachePathForKey(key, path: path)
            if let data = NSData(contentsOfFile: filePath)
            {
                return data
            }
        }
        
        return nil
    }

    private func diskImageForKey(key: String) -> NSData?
    {
        if let data = diskImageDataBySearchingAllPathsForKey(key)
        {
            return data
        }
        
        return nil
    }

    
    private func cachedFileNameForKey(key: String) -> String
    {
        let cstr = key.cStringUsingEncoding(NSUTF8StringEncoding)!
        let strLen = CC_LONG(key.lengthOfBytesUsingEncoding(NSUTF8StringEncoding))
        let digestLen = Int(CC_MD5_DIGEST_LENGTH)
        let result = UnsafeMutablePointer<CUnsignedChar>.alloc(digestLen)
        CC_MD5(cstr, strLen, result)
        let filename = String(format: "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x", result[0], result[1], result[2], result[3], result[4], result[5], result[6], result[7], result[8], result[9], result[10], result[11], result[12], result[13], result[14], result[15])
        result.dealloc(digestLen)
        
        return filename;
    }

    private func IDCacheCostForData(data: NSData) -> Int
    {
        return data.length
    }
    
}
