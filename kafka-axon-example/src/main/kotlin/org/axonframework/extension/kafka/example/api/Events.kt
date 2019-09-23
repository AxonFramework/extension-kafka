/*
 * Copyright (c) 2019. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.axonframework.extension.kafka.example.api

data class BankAccountCreatedEvent(
        val id: String,
        val overdraftLimit: Long
)

sealed class MoneyAddedEvent(
        open val bankAccountId: String,
        open val amount: Long
)

data class MoneyDepositedEvent(override val bankAccountId: String, override val amount: Long) : MoneyAddedEvent(bankAccountId, amount)
data class MoneyOfFailedBankTransferReturnedEvent(override val bankAccountId: String, override val amount: Long) : MoneyAddedEvent(bankAccountId, amount)
data class DestinationBankAccountCreditedEvent(override val bankAccountId: String, override val amount: Long, val bankTransferId: String) : MoneyAddedEvent(bankAccountId, amount)

sealed class MoneySubtractedEvent(
        open val bankAccountId: String,
        open val amount: Long
)

data class MoneyWithdrawnEvent(override val bankAccountId: String, override val amount: Long) : MoneySubtractedEvent(bankAccountId, amount)
data class SourceBankAccountDebitedEvent(override val bankAccountId: String, override val amount: Long, val bankTransferId: String) : MoneySubtractedEvent(bankAccountId, amount)

data class SourceBankAccountDebitRejectedEvent(val bankTransferId: String)