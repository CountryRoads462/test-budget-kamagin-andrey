package mobi.sevenwinds.app.budget

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import mobi.sevenwinds.app.author.AuthorTable
import org.jetbrains.exposed.sql.leftJoin
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.transaction
import org.joda.time.DateTime
import kotlin.streams.toList

object BudgetService {
    suspend fun addRecord(body: BudgetRecord): BudgetRecord = withContext(Dispatchers.IO) {
        transaction {
            val entity = BudgetEntity.new {
                this.year = body.year
                this.month = body.month
                this.amount = body.amount
                this.type = body.type
                this.authorId = body.authorId
            }

            return@transaction entity.toResponse()
        }
    }

    suspend fun getYearStats(param: BudgetYearParam): BudgetYearStatsResponse = withContext(Dispatchers.IO) {
        transaction {
            var query = BudgetTable.leftJoin(AuthorTable, { authorId }, {AuthorTable.id})
                .select { BudgetTable.year eq param.year }
                .limit(param.limit, param.offset)
                .map {
                    BudgetWithAuthor(
                        it[BudgetTable.id].value,
                        it[BudgetTable.year],
                        it[BudgetTable.month],
                        it[BudgetTable.amount],
                        it[BudgetTable.type],
                        it[AuthorTable.fullName],
                        it[AuthorTable.createdAt]
                    )
                }

            if (param.fullName != "") {
                query = query.filter { rec -> rec.authorFullName?.contains(param.fullName, true) ?: false}
            }

            val total = query.count()

            val sumByType = query.groupBy { it.type.name }.mapValues { it.value.sumOf { v -> v.amount } }

            val items = ArrayList<Map<String, Any>>()

            query.forEach { rec ->
                run {
                    val map = LinkedHashMap<String, Any>()

                    map["year"] = rec.year
                    map["month"] = rec.month
                    map["amount"] = rec.amount
                    map["type"] = rec.type
                    if (rec.authorFullName != null) {
                        map["full_name"] = rec.authorFullName
                    }
                    if (rec.authorCreatedAt != null) {
                        map["created_at"] = rec.authorCreatedAt.toString()
                    }

                    items.add(map)
                }
            }

            return@transaction BudgetYearStatsResponse(
                total = total,
                totalByType = sumByType,
                items = items
            )
        }
    }
}

data class BudgetWithAuthor(
    val id: Int,
    val year: Int,
    val month: Int,
    val amount: Int,
    val type: BudgetType,
    val authorFullName: String?,
    val authorCreatedAt: DateTime?
)