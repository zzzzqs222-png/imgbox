/**
 * D1 数据库操作工具类
 */

class D1Database {
    constructor(db) {
        this.db = db;
    }
}

// ==================== D1 批量操作 ====================

/**
 * D1 数据库批量操作 (db.batch)
 * @param {Array<D1PreparedStatement>} statements - D1预处理语句数组
 */
D1Database.prototype.batch = function(statements) {
    return this.db.batch(statements);
};

// ==================== 文件操作 ====================

/**
 * 保存文件记录 (替代 KV.put)
 */
D1Database.prototype.putFile = function(fileId, value, options) {
    value = value || '';
    options = options || {};
    var metadata = options.metadata || {};
    
    // 从metadata中提取字段用于索引
    var extractedFields = this.extractMetadataFields(metadata);
    
    var stmt = this.db.prepare(
        'INSERT OR REPLACE INTO files (' +
        'id, value, metadata, file_name, file_type, file_size, ' +
        'upload_ip, upload_address, list_type, timestamp, ' +
        'label, directory, channel, channel_name, ' +
        'tg_file_id, tg_chat_id, tg_bot_token, is_chunked, tags' + // 添加 tags 字段
        ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    );
    
    // 从 extractedFields 中获取 tags 字段
    const tags = extractedFields.tags || '';

    return stmt.bind(
        fileId,
        value,
        JSON.stringify(metadata),
        extractedFields.fileName,
        extractedFields.fileType,
        extractedFields.fileSize,
        extractedFields.uploadIP,
        extractedFields.uploadAddress,
        extractedFields.listType,
        extractedFields.timestamp,
        extractedFields.label,
        extractedFields.directory,
        extractedFields.channel,
        extractedFields.channelName,
        extractedFields.tgFileId,
        extractedFields.tgChatId,
        extractedFields.tgBotToken,
        extractedFields.isChunked,
        tags
    ).run();
};

/**
 * 获取文件记录 (替代 KV.get)
 */
D1Database.prototype.getFile = function(fileId) {
    var stmt = this.db.prepare('SELECT * FROM files WHERE id = ?');
    return stmt.bind(fileId).first().then(function(result) {
        if (!result) return null;
        
        return {
            value: result.value,
            metadata: JSON.parse(result.metadata || '{}')
        };
    });
};

/**
 * 获取文件记录包含元数据 (替代 KV.getWithMetadata)
 */
D1Database.prototype.getFileWithMetadata = function(fileId) {
    return this.getFile(fileId);
};

/**
 * 获取文件记录包含元数据，仅选择索引所需字段（用于快速索引查询）
 */
D1Database.prototype.getMinimalFileMetadata = function(fileId) {
    // 明确排除 value 字段以加速读取
    var stmt = this.db.prepare(
        'SELECT id, metadata, file_name, file_type, file_size, upload_ip, ' +
        'upload_address, list_type, timestamp, label, directory, channel, ' +
        'channel_name, tg_file_id, tg_chat_id, tg_bot_token, is_chunked, tags ' +
        'FROM files WHERE id = ?'
    );
    return stmt.bind(fileId).first().then(function(result) {
        if (!result) return null;
        
        // 模拟 getWithMetadata 的返回结构
        return {
            name: result.id,
            metadata: JSON.parse(result.metadata || '{}')
            // value 字段在此处被省略
        };
    });
};

/**
 * 删除文件记录 (替代 KV.delete)
 */
D1Database.prototype.deleteFile = function(fileId) {
    var stmt = this.db.prepare('DELETE FROM files WHERE id = ?');
    return stmt.bind(fileId).run();
};

/**
 * 列出文件 (替代 KV.list)
 */
D1Database.prototype.listFiles = function(options) {
    options = options || {};
    var prefix = options.prefix || '';
    var limit = options.limit || 1000;
    var cursor = options.cursor || null;
    
    // 优化：仅选择 id, metadata，排除 value
    var query = 'SELECT id, metadata FROM files';
    var params = [];
    
    if (prefix) {
        query += ' WHERE id LIKE ?';
        params.push(prefix + '%');
    }
    
    if (cursor) {
        query += prefix ? ' AND' : ' WHERE';
        query += ' id > ?';
        params.push(cursor);
    }
    
    query += ' ORDER BY id LIMIT ?';
    params.push(limit + 1);
    
    var stmt = this.db.prepare(query);
    if (params.length > 0) {
        stmt = stmt.bind.apply(stmt, params);
    }
    return stmt.all().then(function(response) {
        var results = response.results || [];
        var hasMore = results.length > limit;
        if (hasMore) {
            results.pop();
        }

        var keys = results.map(function(row) {
            return {
                name: row.id,
                metadata: JSON.parse(row.metadata || '{}')
            };
        });
        
        return {
            keys: keys,
            cursor: hasMore && keys.length > 0 ? keys[keys.length - 1].name : null,
            list_complete: !hasMore
        };
    });
};

// ==================== 设置操作 ====================

/**
 * 保存设置 (替代 KV.put)
 */
D1Database.prototype.putSetting = function(key, value, category) {
    if (!category && key.startsWith('manage@sysConfig@')) {
        category = key.split('@')[2];
    }
    var stmt = this.db.prepare(
        'INSERT OR REPLACE INTO settings (key, value, category) VALUES (?, ?, ?)'
    );
    return stmt.bind(key, value, category).run();
};

/**
 * 获取设置 (替代 KV.get)
 */
D1Database.prototype.getSetting = function(key) {
    var stmt = this.db.prepare('SELECT value FROM settings WHERE key = ?');
    return stmt.bind(key).first().then(function(result) {
        return result ? result.value : null;
    });
};

/**
 * 删除设置 (替代 KV.delete)
 */
D1Database.prototype.deleteSetting = function(key) {
    var stmt = this.db.prepare('DELETE FROM settings WHERE key = ?');
    return stmt.bind(key).run();
};

/**
 * 列出设置 (替代 KV.list)
 */
D1Database.prototype.listSettings = function(options) {
    options = options || {};
    var prefix = options.prefix || '';
    var limit = options.limit || 1000;
    var query = 'SELECT key, value FROM settings';
    var params = [];
    if (prefix) {
        query += ' WHERE key LIKE ?';
        params.push(prefix.replace('manage@sysConfig@', '') + '%');
    }
    query += ' ORDER BY key LIMIT ?';
    params.push(limit);
    
    var stmt = this.db.prepare(query);
    if (params.length > 0) {
        stmt = stmt.bind.apply(stmt, params);
    }
    return stmt.all().then(function(response) {
        var results = response.results || [];
        var hasMore = results.length > limit; // D1.all() 并不返回 hasMore，这里是模拟 KV 的行为
        
        var keys = results.map(function(row) {
            return {
                name: 'manage@sysConfig@' + row.key,
                value: row.value
            };
        });
        
        return {
            keys: keys,
            cursor: null,
            list_complete: true
        };
    });
};

// ==================== 其他数据操作 (用于索引分块) ====================

/**
 * 保存其他数据 (用于索引分块 - 替代 KV.put)
 */
D1Database.prototype.putOtherData = function(key, value, type) {
    var stmt = this.db.prepare(
        'INSERT OR REPLACE INTO other_data (key, value, type) VALUES (?, ?, ?)'
    );
    return stmt.bind(key, value, type || 'index_chunk').run();
};

/**
 * 获取其他数据 (用于索引分块 - 替代 KV.get)
 */
D1Database.prototype.getOtherData = function(key) {
    var stmt = this.db.prepare('SELECT value FROM other_data WHERE key = ?');
    return stmt.bind(key).first().then(function(result) {
        return result ? result.value : null;
    });
};

/**
 * 删除其他数据 (用于索引分块 - 替代 KV.delete)
 */
D1Database.prototype.deleteOtherData = function(key) {
    var stmt = this.db.prepare('DELETE FROM other_data WHERE key = ?');
    return stmt.bind(key).run();
};


// ==================== 索引操作 (Index Operations) ====================

/**
 * 记录索引操作 (替代 KV.put for manage@index@operation_...)
 */
D1Database.prototype.putIndexOperation = function(operationId, operationData) {
    var stmt = this.db.prepare(
        'INSERT INTO index_operations (id, type, timestamp, data) VALUES (?, ?, ?, ?)'
    );
    return stmt.bind(
        operationId,
        operationData.type,
        operationData.timestamp,
        JSON.stringify(operationData.data)
    ).run();
};


/**
 * 删除索引操作
 */
D1Database.prototype.deleteIndexOperation = function(operationId) {
    var stmt = this.db.prepare('DELETE FROM index_operations WHERE id = ?');
    return stmt.bind(operationId).run();
};

/**
 * 列出索引操作
 */
D1Database.prototype.listIndexOperations = function(options) {
    options = options || {};
    var prefix = options.prefix || '';
    var limit = options.limit || 1000;
    var processed = options.processed; // D1特有
    var lastOperationId = options.lastOperationId; // D1特有
    
    var query = 'SELECT id, type, timestamp, data, processed FROM index_operations';
    var params = [];
    var conditions = [];

    if (processed !== undefined && processed !== null) {
        conditions.push('processed = ?');
        params.push(processed);
    }
    
    if (lastOperationId) {
        // 仅获取比上一个操作ID更新的操作
        // 这是一个近似值，更精确的应该是基于 timestamp + id
        conditions.push('id > ?');
        params.push(lastOperationId);
    }

    if (conditions.length > 0) {
        query += ' WHERE ' + conditions.join(' AND ');
    }
    
    query += ' ORDER BY timestamp, id LIMIT ?';
    params.push(limit);

    var stmt = this.db.prepare(query);
    if (params.length > 0) {
        stmt = stmt.bind.apply(stmt, params);
    }
    return stmt.all().then(function(response) {
        var results = response.results || [];
        return results.map(function(row) {
            return {
                id: row.id,
                type: row.type,
                timestamp: row.timestamp,
                data: JSON.parse(row.data),
                processed: row.processed
            };
        });
    });
};

/**
 * 获取最后一个已处理的索引操作 (用于 rebuildIndex)
 */
D1Database.prototype.getLastProcessedIndexOperation = function() {
    var stmt = this.db.prepare(
        'SELECT id, timestamp FROM index_operations WHERE processed = TRUE ORDER BY timestamp DESC, id DESC LIMIT 1'
    );
    return stmt.first().then(function(result) {
        return result ? { id: result.id, timestamp: result.timestamp } : null;
    });
};


// ==================== 统一接口实现 ====================

/**
 * 通用的put方法 (替代 KV.put)
 */
D1Database.prototype.put = function(key, value, options) {
    options = options || {};
    
    if (key.startsWith('manage@sysConfig@')) {
        return this.putSetting(key.replace('manage@sysConfig@', ''), value);
    } else if (key.startsWith('manage@index@operation_')) {
        var operationId = key.replace('manage@index@operation_', '');
        // 假定 value 是 JSON 字符串，需要解析为 operationData
        var operationData;
        try {
            operationData = JSON.parse(value);
        } catch (e) {
            console.error('Invalid operation data for put:', value);
            return Promise.reject(new Error('Invalid operation data'));
        }
        return this.putIndexOperation(operationId, operationData);
    } else if (key.startsWith('manage@index_')) {
        // 索引分块
        return this.putOtherData(key, value, 'index_chunk');
    } else if (key === 'manage@index@meta') {
        // 索引元数据 (虽然建议使用 index_metadata 表，但为了兼容KV接口，这里使用 other_data)
        return this.putOtherData(key, value, 'index_meta');
    } else {
        // 默认是文件
        return this.putFile(key, value, options);
    }
};

/**
 * 通用的get方法 (替代 KV.get)
 */
D1Database.prototype.get = function(key) {
    var self = this;
    
    if (key.startsWith('manage@sysConfig@')) {
        return this.getSetting(key.replace('manage@sysConfig@', ''));
    } else if (key.startsWith('manage@index@operation_')) {
        // 获取原子操作的原始 value (JSON字符串)
        var operationId = key.replace('manage@index@operation_', '');
        return this.db.prepare('SELECT data FROM index_operations WHERE id = ?')
                   .bind(operationId).first().then(function(result) {
            if (!result) return null;
            
            // 重新组合为原始 KV 的 value 格式 (JSON string)
            return JSON.stringify({ data: JSON.parse(result.data) });
        });
    } else if (key.startsWith('manage@index_')) {
        // 索引分块
        return this.getOtherData(key);
    } else if (key === 'manage@index@meta') {
        // 索引元数据
        return this.getOtherData(key);
    } else {
        // 文件
        return this.getFile(key).then(function(result) {
            return result ? result.value : null;
        });
    }
};

/**
 * 通用的getWithMetadata方法
 */
D1Database.prototype.getWithMetadata = function(key) {
    var self = this;

    if (key.startsWith('manage@sysConfig@')) {
        return this.getSetting(key).then(function(value) {
            return value ? { value: value, metadata: {} } : null;
        });
    } else if (key.startsWith('manage@index@operation_')) {
        // 原子操作不返回元数据
        return this.get(key).then(function(value) {
            return value ? { value: value, metadata: {} } : null;
        });
    } else if (key.startsWith('manage@index_') || key === 'manage@index@meta') {
        // 索引数据不返回元数据
        return this.get(key).then(function(value) {
            return value ? { value: value, metadata: {} } : null;
        });
    } else {
        return this.getFileWithMetadata(key);
    }
};

/**
 * 通用的delete方法
 */
D1Database.prototype.delete = function(key) {
    if (key.startsWith('manage@sysConfig@')) {
        return this.deleteSetting(key.replace('manage@sysConfig@', ''));
    } else if (key.startsWith('manage@index@operation_')) {
        var operationId = key.replace('manage@index@operation_', '');
        return this.deleteIndexOperation(operationId);
    } else if (key.startsWith('manage@index_')) {
        return this.deleteOtherData(key);
    } else if (key === 'manage@index@meta') {
        return this.deleteOtherData(key);
    } else {
        return this.deleteFile(key);
    }
};

/**
 * 通用的list方法
 */
D1Database.prototype.list = function(options) {
    options = options || {};
    var prefix = options.prefix || '';
    var self = this;

    if (prefix.startsWith('manage@sysConfig@')) {
        return this.listSettings(options);
    } else if (prefix.startsWith('manage@index@operation_')) {
        // 这里返回的操作列表不包含 value，只包含 name (KV key)
        return this.listIndexOperations(options).then(function(operations) {
            var keys = operations.map(function(op) {
                return {
                    name: 'manage@index@operation_' + op.id
                };
            });
            // 模拟 KV list 的返回结构
            return {
                keys: keys,
                cursor: null,
                list_complete: true
            };
        });
    } else {
        return this.listFiles(options);
    }
};

// ==================== 工具方法 ====================

/**
 * 从metadata中提取字段用于索引
 */
D1Database.prototype.extractMetadataFields = function(metadata) {
    // 确保与文件表中的字段匹配
    return {
        fileName: metadata.FileName || null,
        fileType: metadata.FileType || null,
        fileSize: metadata.FileSize || null,
        uploadIP: metadata.UploadIP || null,
        uploadAddress: metadata.UploadAddress || null,
        listType: metadata.ListType || null,
        timestamp: metadata.TimeStamp || Math.floor(Date.now() / 1000),
        label: metadata.Label || null,
        directory: metadata.Directory || null,
        channel: metadata.Channel || null,
        channelName: metadata.ChannelName || null,
        tgFileId: metadata.TgFileId || null,
        tgChatId: metadata.tgChatId || null,
        tgBotToken: metadata.tgBotToken || null,
        isChunked: metadata.IsChunked || false,
        tags: metadata.Tags || null // 新增 tags
    };
};

export { D1Database };

// /**
//  * D1 数据库操作工具类
//  */

// class D1Database {
//     constructor(db) {
//         this.db = db;
//     }
// }

// // ==================== 文件操作 ====================

// /**
//  * 保存文件记录 (替代 KV.put)
//  */
// D1Database.prototype.putFile = function(fileId, value, options) {
//     value = value || '';
//     options = options || {};
//     var metadata = options.metadata || {};
    
//     // 从metadata中提取字段用于索引
//     var extractedFields = this.extractMetadataFields(metadata);
    
//     var stmt = this.db.prepare(
//         'INSERT OR REPLACE INTO files (' +
//         'id, value, metadata, file_name, file_type, file_size, ' +
//         'upload_ip, upload_address, list_type, timestamp, ' +
//         'label, directory, channel, channel_name, ' +
//         'tg_file_id, tg_chat_id, tg_bot_token, is_chunked' +
//         ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
//     );
    
//     return stmt.bind(
//         fileId,
//         value,
//         JSON.stringify(metadata),
//         extractedFields.fileName,
//         extractedFields.fileType,
//         extractedFields.fileSize,
//         extractedFields.uploadIP,
//         extractedFields.uploadAddress,
//         extractedFields.listType,
//         extractedFields.timestamp,
//         extractedFields.label,
//         extractedFields.directory,
//         extractedFields.channel,
//         extractedFields.channelName,
//         extractedFields.tgFileId,
//         extractedFields.tgChatId,
//         extractedFields.tgBotToken,
//         extractedFields.isChunked
//     ).run();
// };

// /**
//  * 获取文件记录 (替代 KV.get)
//  */
// D1Database.prototype.getFile = function(fileId) {
//     var self = this;
//     var stmt = this.db.prepare('SELECT * FROM files WHERE id = ?');
//     return stmt.bind(fileId).first().then(function(result) {
//         if (!result) return null;
        
//         return {
//             value: result.value,
//             metadata: JSON.parse(result.metadata || '{}')
//         };
//     });
// };

// /**
//  * 获取文件记录包含元数据 (替代 KV.getWithMetadata)
//  */
// D1Database.prototype.getFileWithMetadata = function(fileId) {
//     return this.getFile(fileId);
// };

// /**
//  * 删除文件记录 (替代 KV.delete)
//  */
// D1Database.prototype.deleteFile = function(fileId) {
//     var stmt = this.db.prepare('DELETE FROM files WHERE id = ?');
//     return stmt.bind(fileId).run();
// };

// /**
//  * 列出文件 (替代 KV.list)
//  */
// D1Database.prototype.listFiles = function(options) {
//     options = options || {};
//     var prefix = options.prefix || '';
//     var limit = options.limit || 1000;
//     var cursor = options.cursor || null;
    
//     var query = 'SELECT id, metadata FROM files';
//     var params = [];
    
//     if (prefix) {
//         query += ' WHERE id LIKE ?';
//         params.push(prefix + '%');
//     }
    
//     if (cursor) {
//         query += prefix ? ' AND' : ' WHERE';
//         query += ' id > ?';
//         params.push(cursor);
//     }
    
//     query += ' ORDER BY id LIMIT ?';
//     params.push(limit + 1);
    
//     var stmt = this.db.prepare(query);
//     if (params.length > 0) {
//         stmt = stmt.bind.apply(stmt, params);
//     }
//     return stmt.all().then(function(response) {
//         var results = response.results || [];
//         var hasMore = results.length > limit;
//         if (hasMore) {
//             results.pop();
//         }

//         var keys = results.map(function(row) {
//             return {
//                 name: row.id,
//                 metadata: JSON.parse(row.metadata || '{}')
//             };
//         });
        
//         return {
//             keys: keys,
//             cursor: hasMore && keys.length > 0 ? keys[keys.length - 1].name : null,
//             list_complete: !hasMore
//         };
//     });
// };

// // ==================== 设置操作 ====================

// /**
//  * 保存设置 (替代 KV.put)
//  */
// D1Database.prototype.putSetting = function(key, value, category) {
//     if (!category && key.startsWith('manage@sysConfig@')) {
//         category = key.split('@')[2];
//     }
    
//     var stmt = this.db.prepare(
//         'INSERT OR REPLACE INTO settings (key, value, category) VALUES (?, ?, ?)'
//     );
    
//     return stmt.bind(key, value, category).run();
// };

// /**
//  * 获取设置 (替代 KV.get)
//  */
// D1Database.prototype.getSetting = function(key) {
//     var stmt = this.db.prepare('SELECT value FROM settings WHERE key = ?');
//     return stmt.bind(key).first().then(function(result) {
//         return result ? result.value : null;
//     });
// };

// /**
//  * 删除设置 (替代 KV.delete)
//  */
// D1Database.prototype.deleteSetting = function(key) {
//     var stmt = this.db.prepare('DELETE FROM settings WHERE key = ?');
//     return stmt.bind(key).run();
// };

// /**
//  * 列出设置 (替代 KV.list)
//  */
// D1Database.prototype.listSettings = function(options) {
//     options = options || {};
//     var prefix = options.prefix || '';
//     var limit = options.limit || 1000;
    
//     var query = 'SELECT key, value FROM settings';
//     var params = [];
    
//     if (prefix) {
//         query += ' WHERE key LIKE ?';
//         params.push(prefix + '%');
//     }
    
//     query += ' ORDER BY key LIMIT ?';
//     params.push(limit);
    
//     var stmt = this.db.prepare(query);
//     if (params.length > 0) {
//         stmt = stmt.bind.apply(stmt, params);
//     }
//     return stmt.all().then(function(response) {
//         var results = response.results || [];
//         var keys = results.map(function(row) {
//             return {
//                 name: row.key,
//                 value: row.value
//             };
//         });

//         return { keys: keys };
//     });
// };

// // ==================== 索引操作 ====================

// /**
//  * 保存索引操作记录
//  */
// D1Database.prototype.putIndexOperation = function(operationId, operation) {
//     var stmt = this.db.prepare(
//         'INSERT OR REPLACE INTO index_operations (id, type, timestamp, data) VALUES (?, ?, ?, ?)'
//     );
    
//     return stmt.bind(
//         operationId,
//         operation.type,
//         operation.timestamp,
//         JSON.stringify(operation.data)
//     ).run();
// };

// /**
//  * 获取索引操作记录
//  */
// D1Database.prototype.getIndexOperation = function(operationId) {
//     var stmt = this.db.prepare('SELECT * FROM index_operations WHERE id = ?');
//     return stmt.bind(operationId).first().then(function(result) {
//         if (!result) return null;
        
//         return {
//             type: result.type,
//             timestamp: result.timestamp,
//             data: JSON.parse(result.data)
//         };
//     });
// };

// /**
//  * 删除索引操作记录
//  */
// D1Database.prototype.deleteIndexOperation = function(operationId) {
//     var stmt = this.db.prepare('DELETE FROM index_operations WHERE id = ?');
//     return stmt.bind(operationId).run();
// };

// /**
//  * 列出索引操作记录
//  */
// D1Database.prototype.listIndexOperations = function(options) {
//     options = options || {};
//     var limit = options.limit || 1000;
//     var processed = options.processed;
    
//     var query = 'SELECT * FROM index_operations';
//     var params = [];
    
//     if (processed !== null && processed !== undefined) {
//         query += ' WHERE processed = ?';
//         params.push(processed);
//     }
    
//     query += ' ORDER BY timestamp LIMIT ?';
//     params.push(limit);
    
//     var stmt = this.db.prepare(query);
//     if (params.length > 0) {
//         stmt = stmt.bind.apply(stmt, params);
//     }
//     return stmt.all().then(function(response) {
//         var results = response.results || [];
//         return results.map(function(row) {
//             return {
//                 id: row.id,
//                 type: row.type,
//                 timestamp: row.timestamp,
//                 data: JSON.parse(row.data),
//                 processed: row.processed
//             };
//         });
//     });
// };

// // ==================== 工具方法 ====================

// /**
//  * 从metadata中提取字段用于索引
//  */
// D1Database.prototype.extractMetadataFields = function(metadata) {
//     return {
//         fileName: metadata.FileName || null,
//         fileType: metadata.FileType || null,
//         fileSize: metadata.FileSize || null,
//         uploadIP: metadata.UploadIP || null,
//         uploadAddress: metadata.UploadAddress || null,
//         listType: metadata.ListType || null,
//         timestamp: metadata.TimeStamp || null,
//         label: metadata.Label || null,
//         directory: metadata.Directory || null,
//         channel: metadata.Channel || null,
//         channelName: metadata.ChannelName || null,
//         tgFileId: metadata.TgFileId || null,
//         tgChatId: metadata.TgChatId || null,
//         tgBotToken: metadata.TgBotToken || null,
//         isChunked: metadata.IsChunked || false
//     };
// };

// // ==================== 通用方法 ====================

// /**
//  * 通用的put方法，根据key类型自动选择存储位置
//  */
// D1Database.prototype.put = function(key, value, options) {
//     options = options || {};

//     if (key.startsWith('manage@sysConfig@')) {
//         return this.putSetting(key, value);
//     } else if (key.startsWith('manage@index@operation_')) {
//         var operationId = key.replace('manage@index@operation_', '');
//         var operation = JSON.parse(value);
//         return this.putIndexOperation(operationId, operation);
//     } else {
//         return this.putFile(key, value, options);
//     }
// };

// /**
//  * 通用的get方法，根据key类型自动选择获取位置
//  */
// D1Database.prototype.get = function(key) {
//     var self = this;

//     if (key.startsWith('manage@sysConfig@')) {
//         return this.getSetting(key);
//     } else if (key.startsWith('manage@index@operation_')) {
//         var operationId = key.replace('manage@index@operation_', '');
//         return this.getIndexOperation(operationId).then(function(operation) {
//             return operation ? JSON.stringify(operation) : null;
//         });
//     } else {
//         return this.getFile(key).then(function(file) {
//             return file ? file.value : null;
//         });
//     }
// };

// /**
//  * 通用的getWithMetadata方法
//  */
// D1Database.prototype.getWithMetadata = function(key) {
//     var self = this;

//     if (key.startsWith('manage@sysConfig@')) {
//         return this.getSetting(key).then(function(value) {
//             return value ? { value: value, metadata: {} } : null;
//         });
//     } else {
//         return this.getFileWithMetadata(key);
//     }
// };

// /**
//  * 通用的delete方法
//  */
// D1Database.prototype.delete = function(key) {
//     if (key.startsWith('manage@sysConfig@')) {
//         return this.deleteSetting(key);
//     } else if (key.startsWith('manage@index@operation_')) {
//         var operationId = key.replace('manage@index@operation_', '');
//         return this.deleteIndexOperation(operationId);
//     } else {
//         return this.deleteFile(key);
//     }
// };

// /**
//  * 通用的list方法
//  */
// D1Database.prototype.list = function(options) {
//     options = options || {};
//     var prefix = options.prefix || '';
//     var self = this;

//     if (prefix.startsWith('manage@sysConfig@')) {
//         return this.listSettings(options);
//     } else if (prefix.startsWith('manage@index@operation_')) {
//         return this.listIndexOperations(options).then(function(operations) {
//             var keys = operations.map(function(op) {
//                 return {
//                     name: 'manage@index@operation_' + op.id
//                 };
//             });
//             return { keys: keys };
//         });
//     } else {
//         return this.listFiles(options);
//     }
// };

// // 导出构造函数
// export { D1Database };
