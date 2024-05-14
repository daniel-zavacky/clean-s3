import { QueryTypes } from 'sequelize'
import { find, forEach, map, reduce, replace } from 'lodash'
import dayjs from 'dayjs'
import { DeleteObjectsCommand, _Error } from '@aws-sdk/client-s3'
import config from 'config'

import sequelize from './example/src/db/models'

// utils
import { uglifyRawSqlQuery } from './example/src/utils/helper'

// services
import { s3Client } from './example/src/services/awsS3Service'

// types
import { IAwsConfig } from './example/src/types/config'

const awsConfig: IAwsConfig = config.get('aws')

/**
 * Selects files, which are not used in any association and marks them for deletion (by soft-deleting them)
 * @param {string} filesTableName
 * @param {string} primaryKeyColumnName
 * @returns {Promise<number>} Count of files marked for deletion
 */
const markFilesForDeletion = async (filesTableName: string, primaryKeyColumnName: string) => {
	// get all associations where file is a foreign key
	const rawAssociations = await sequelize.query<{
		constraint_name: string
		table_name: string
		column_name: string
	}>(
		uglifyRawSqlQuery(/* SQL */ `
			SELECT DISTINCT
				"tc"."constraint_name",
				--"tc"."table_schema",
				"tc"."table_name",
				"kcu"."column_name"
				--"ccu"."table_schema" AS "foreign_table_schema",
				--"ccu"."table_name" AS "foreign_table_name",
				--"ccu"."column_name" AS "foreign_column_name"
			FROM "information_schema"."table_constraints" AS "tc"
			INNER JOIN "information_schema"."key_column_usage" AS "kcu" ON "tc"."constraint_name" = "kcu"."constraint_name"
				AND ( "tc"."table_schema" = "kcu"."table_schema" )
			INNER JOIN "information_schema"."constraint_column_usage" AS "ccu" ON "ccu"."constraint_name" = "tc"."constraint_name"
				AND ( "ccu"."table_schema" = "tc"."table_schema" )
			WHERE (
				"tc"."constraint_type" = 'FOREIGN KEY'
				AND "ccu"."table_name" = :filesTableName
			)
		`),
		{
			type: QueryTypes.SELECT,
			replacements: {
				filesTableName
			}
		}
	)

	// check if files table has associations
	const associatedTablesDescribeData = await Promise.all(map(rawAssociations, (rawAssociation) => sequelize.getQueryInterface().describeTable(rawAssociation.table_name)))

	const associations = map(rawAssociations, (rawAssociation, index) => ({
		table: rawAssociation.table_name as string,
		as: rawAssociation.constraint_name as string,
		fileColumn: rawAssociation.column_name as string,
		hasDeletedAt: !!associatedTablesDescribeData[index].deletedAt
	}))
	if (associations.length === 0) {
		throw new Error('Files table does not contain any associations')
	}

	// get files which are unused (does not have any association)
	const unusedFilesResult = await sequelize.query(
		uglifyRawSqlQuery(/* SQL */ `
			UPDATE "${filesTableName}"
			SET
				"deletedAt" = now()
			FROM (
				SELECT
					"${filesTableName}"."${primaryKeyColumnName}"
				FROM "${filesTableName}"
				WHERE (
					"${filesTableName}"."deletedAt" IS NULL
					${
						associations.length > 0
							? /* SQL */ ` AND
								${map(associations, (association) => /* SQL */ `NOT exists(
									SELECT
										1
									FROM "${association.table}" AS "${association.as}"
									WHERE (
										"${association.as}"."${association.fileColumn}" = "${filesTableName}"."${primaryKeyColumnName}"
									)
								`).join(' AND ')}
							`
							: ''
					}
				)
			)  AS "unusedFile"
			WHERE (
				"${filesTableName}"."${primaryKeyColumnName}" = "unusedFile"."${primaryKeyColumnName}"
			)
		`),
		{
			type: QueryTypes.BULKUPDATE
		}
	)

	return {
		unused: unusedFilesResult
	}
}

/**
 * Deletes files (hard-delete from database + delete from aws s3) which were marked for deletion 30 days ago
 * @param {string} filesTableName
 * @param {string} primaryKeyColumnName
 * @param {string} keyColumnName
 * @param {string} keyColumnBase
 * @returns {Promise<{ errors: _Error[], removed: number }>
 */
const removeFiles = async (filesTableName: string, primaryKeyColumnName: string, keyColumnName: string, keyColumnBase: string) => {
	const todayMinus30Days = dayjs().subtract(30, 'days').toISOString()

	const errors: _Error[] = []
	let removed = 0

	const limit = 1000

	// get count of all files which should be hard-deleted at current date (they were soft-deleted before 30 days from today)
	const deleteFilesCountResult = await sequelize.query<{ count: number }>(
		uglifyRawSqlQuery(/* SQL */ `
			SELECT
				count( * ) AS "count"
			FROM "${filesTableName}"
			WHERE (
				"${filesTableName}"."deletedAt" <= :todayMinus30Days
			)
		`),
		{
			type: QueryTypes.SELECT,
			replacements: {
				todayMinus30Days
			}
		}
	)

	// calculate number of iterations to delete all files
	const deleteFilesCount = deleteFilesCountResult[0]?.count ?? 0
	const chunkCount = Math.ceil(deleteFilesCount / limit)
	const pages = map(Array.from(Array(chunkCount)), (_value, index) => index + 1)

	/**
	 * set starting point for cursor ("ASC order" and "> comparison" on primary key column),
	 * which is used to paginate through all files which should be deleted.
	 * 
	 * iteration by primary key column is used, since we need unique value to paginate through all files because not all selected files are deleted (only files deleted from aws s3 are deleted from database).
	 * if for example "createdAt" would be used, we could have multiple files with same "createdAt" value, which would break logic
	 * @example
	 * deleteFilesCount=5, limit=2:
	 * |1|1| |1|2| |2|
	 * |o|n| | | | | |
	 * if we would use "createdAt" and 2. file from 1. iteration would not be deleted from aws s3 (and hence from db), we would still have lastProcessedID=1 and we would process 2. file again and skip 5. file
	 */
	let lastProcessedID: string | null = null

	// delete files in chunks serially (one by one)
	await reduce(
		pages,
		(promise, page) => {
			return promise.then(async () => {
				// lastProcessedID=null can be only on first iteration
				if (!lastProcessedID && page !== 1) {
					throw new Error('cursor (lastProcessedID) is not set')
				}

				// get all files which should be deleted at current date (they were deleted before 30 days from today)
				const deleteFiles = await sequelize.query<{
					primaryKeyColumnName: string
					keyColumnName: string
				}>(
					uglifyRawSqlQuery(/* SQL */ `
						SELECT
							"${filesTableName}"."${primaryKeyColumnName}" AS "primaryKeyColumnName",
							"${filesTableName}"."${keyColumnName}" AS "keyColumnName"
						FROM "${filesTableName}"
						WHERE (
							"${filesTableName}"."deletedAt" <= :todayMinus30Days
							${lastProcessedID ? /* SQL */ `AND "${filesTableName}"."${primaryKeyColumnName}" > :lastProcessedID` : ''}
						)
						ORDER BY "${filesTableName}"."${primaryKeyColumnName}" ASC
						LIMIT ${limit}
					`),
					{
						type: QueryTypes.SELECT,
						replacements: {
							todayMinus30Days,
							lastProcessedID
						}
					}
				)

				lastProcessedID = deleteFiles[deleteFiles.length - 1].primaryKeyColumnName

				// remove files which are marked for delete at current date (s3)
				const s3DeleteObjects = map(deleteFiles, (deleteFile) => ({ Key: replace(deleteFile.keyColumnName, new RegExp(keyColumnBase), '') }))
				const awsRemovedKeys: string[] = []

				const command = new DeleteObjectsCommand({
					Bucket: awsConfig.s3.bucket,
					Delete: {
						Objects: s3DeleteObjects
					}
				})
				const deleteResult = await s3Client.send(command)

				forEach(deleteResult.Deleted, (deletedObject) => {
					if (deletedObject?.Key) {
						awsRemovedKeys.push(deletedObject.Key)
					}
				})

				forEach(deleteResult.Errors, (error) => {
					errors.push(error)
				})

				// remove files which are marked for delete at current date and were removed in aws (db)
				if (awsRemovedKeys.length > 0) {
					const dbDeleteFilePaths = map(awsRemovedKeys, (awsRemovedKey) => `${keyColumnBase}${awsRemovedKey}`)

					await sequelize.query(
						uglifyRawSqlQuery(/* SQL */ `
							DELETE
							FROM "${filesTableName}"
							WHERE (
								"${filesTableName}"."${keyColumnName}" IN ( :dbDeleteFilePaths )
							)
						`),
						{
							type: QueryTypes.BULKDELETE,
							replacements: {
								dbDeleteFilePaths
							}
						}
					)
				}

				removed += awsRemovedKeys.length

				return Promise.resolve()
			})
		},
		Promise.resolve()
	)

	return {
		errors,
		removed
	}
}

export default async () => {
	try {
		// #region perform checks
		const filesTableName = process.env.S3_CLEANUP_FILES_TABLE_NAME
		if (!filesTableName) {
			throw new Error('S3_CLEANUP_FILES_TABLE_NAME env not provided')
		}

		const keyColumnName = process.env.S3_CLEANUP_KEY_COLUMN_NAME
		if (!keyColumnName) {
			throw new Error('S3_CLEANUP_KEY_COLUMN_NAME env not provided')
		}

		const keyColumnBase = process.env.S3_CLEANUP_KEY_COLUMN_BASE || ''

		const filesTableDescribeData = await sequelize.getQueryInterface().describeTable(filesTableName)

		// check if files table has primaryKey
		const columns = map(filesTableDescribeData, (column, columnName) => ({
			...column,
			name: columnName
		}))
		const primaryKeyColumn = find(columns, (column) => column.primaryKey === true)
		if (!primaryKeyColumn) {
			throw new Error('Files table does not have primary key')
		}
		if (!['uuid', 'integer', 'bigint'].includes(primaryKeyColumn.type)) {
			throw new Error('Primary key type is not supported')
		}
		const primaryKeyColumnName = primaryKeyColumn.name

		// check if files table has provided column
		if (!filesTableDescribeData[keyColumnName]) {
			throw new Error('Files table does not have keyColumnName')
		}

		// check if files table has deletedAt column
		if (!filesTableDescribeData.deletedAt) {
			throw new Error('Files table does not have deletedAt column')
		}
		// #endregion

		// NOTE: mark for deletion files which are not used (they do not have any association)
		const unusedData = await markFilesForDeletion(filesTableName, primaryKeyColumnName)

		// NOTE: delete (from db and s3) files which were mark for deletion 30 days ago
		const removedData = await removeFiles(filesTableName, primaryKeyColumnName, keyColumnName, keyColumnBase)

		return {
			unused: unusedData.unused,
			deleted: removedData.removed,
			errors: removedData.errors
		}
	} catch (error) {
		return Promise.reject(error)
	}
}
