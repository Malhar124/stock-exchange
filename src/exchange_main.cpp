#include <iostream>
#include <string_view>

#include "core/types.h"
#include "matching/events.h"
#include "matching/matching_engine.h"

namespace {

using exchange::core::OrderType;
using exchange::core::Side;
using exchange::matching::Event;
using exchange::matching::EventType;
using exchange::matching::ReasonCode;

[[nodiscard]] std::string_view side_name(Side side) noexcept {
	return side == Side::BUY ? "BUY" : "SELL";
}

[[nodiscard]] std::string_view order_type_name(OrderType type) noexcept {
	switch (type) {
	case OrderType::LIMIT:
		return "LIMIT";
	case OrderType::MARKET:
		return "MARKET";
	case OrderType::IOC:
		return "IOC";
	case OrderType::FOK:
		return "FOK";
	case OrderType::GTC:
		return "GTC";
	case OrderType::STOP:
		return "STOP";
	case OrderType::STOP_LIMIT:
		return "STOP_LIMIT";
	case OrderType::ICEBERG:
		return "ICEBERG";
	case OrderType::POST_ONLY:
		return "POST_ONLY";
	}
	return "UNKNOWN";
}

[[nodiscard]] std::string_view reason_name(ReasonCode reason) noexcept {
	switch (reason) {
	case ReasonCode::NONE:
		return "NONE";
	case ReasonCode::ZERO_QUANTITY:
		return "ZERO_QUANTITY";
	case ReasonCode::NEGATIVE_PRICE:
		return "NEGATIVE_PRICE";
	case ReasonCode::BOOK_EMPTY:
		return "BOOK_EMPTY";
	case ReasonCode::DUPLICATE_ORDER_ID:
		return "DUPLICATE_ORDER_ID";
	case ReasonCode::ORDER_NOT_FOUND:
		return "ORDER_NOT_FOUND";
	case ReasonCode::ORDER_NOT_ACTIVE:
		return "ORDER_NOT_ACTIVE";
	case ReasonCode::FOK_INSUFFICIENT_LIQUIDITY:
		return "FOK_INSUFFICIENT_LIQUIDITY";
	case ReasonCode::POST_ONLY_WOULD_CROSS:
		return "POST_ONLY_WOULD_CROSS";
	case ReasonCode::INVALID_MODIFICATION:
		return "INVALID_MODIFICATION";
	case ReasonCode::INVALID_ICEBERG_DISPLAY:
		return "INVALID_ICEBERG_DISPLAY";
	case ReasonCode::ORDER_POOL_EXHAUSTED:
		return "ORDER_POOL_EXHAUSTED";
	}
	return "UNKNOWN";
}

void print_event(const Event &event) {
	switch (event.type) {
	case EventType::ORDER_ACCEPTED: {
		const auto &value = event.payload.order_accepted;
		std::cout << "  ACCEPTED order=" << value.order_id
							<< " side=" << side_name(value.side)
							<< " price=" << value.price << " qty=" << value.qty
							<< " type=" << order_type_name(value.order_type) << '\n';
		break;
	}
	case EventType::ORDER_RESTED: {
		const auto &value = event.payload.order_rested;
		std::cout << "  RESTED order=" << value.order_id
							<< " side=" << side_name(value.side)
							<< " price=" << value.price << " qty=" << value.qty << '\n';
		break;
	}
	case EventType::ORDER_REJECTED: {
		const auto &value = event.payload.order_rejected;
		std::cout << "  REJECTED order=" << value.order_id
							<< " reason=" << reason_name(value.reason_code) << '\n';
		break;
	}
	case EventType::ORDER_REDUCED: {
		const auto &value = event.payload.order_reduced;
		std::cout << "  REDUCED order=" << value.order_id
							<< " new_qty=" << value.new_qty << '\n';
		break;
	}
	case EventType::ORDER_PARTIALLY_FILLED: {
		const auto &value = event.payload.order_partially_filled;
		std::cout << "  PARTIALLY_FILLED order=" << value.order_id
							<< " filled=" << value.filled_qty
							<< " remaining=" << value.remaining_qty
							<< " price=" << value.price << '\n';
		break;
	}
	case EventType::ORDER_FILLED: {
		const auto &value = event.payload.order_filled;
		std::cout << "  FILLED order=" << value.order_id
							<< " qty=" << value.filled_qty << " price=" << value.price
							<< '\n';
		break;
	}
	case EventType::ORDER_CANCELED: {
		const auto &value = event.payload.order_canceled;
		std::cout << "  CANCELED order=" << value.order_id
							<< " qty=" << value.canceled_qty << '\n';
		break;
	}
	case EventType::TRADE: {
		const auto &value = event.payload.trade;
		std::cout << "  TRADE match=" << value.match_id
							<< " buy_order=" << value.buy_order_id
							<< " sell_order=" << value.sell_order_id
							<< " price=" << value.price << " qty=" << value.qty << '\n';
		break;
	}
	}
}

void print_result(std::string_view description,
									std::span<const Event> events) {
	std::cout << description << " (" << events.size() << " events)\n";
	for (const Event &event : events) {
		print_event(event);
	}
}

} // namespace

int main() {
	using namespace exchange;

	matching::MatchingEngine<> engine;
	const core::Symbol symbol = core::make_symbol("AAPL");
	engine.prepare_symbols(std::span<const core::Symbol>(&symbol, 1));

	const auto submit = [&](matching::InboundOrder order,
													std::string_view description) {
		print_result(description, engine.process_order(order));
	};

	submit(matching::InboundOrder{
						 .action = matching::RequestAction::ADD,
						 .symbol = symbol,
						 .order_id = 1001,
						 .side = core::Side::SELL,
						 .price = 1'010'000,
						 .qty = 100,
						 .order_type = core::OrderType::LIMIT,
						 .timestamp = 1,
						 .participant_id = 2,
				 },
				 "1. Add resting ask");

	submit(matching::InboundOrder{
						 .action = matching::RequestAction::ADD,
						 .symbol = symbol,
						 .order_id = 2001,
						 .side = core::Side::BUY,
						 .price = 1'000'000,
						 .qty = 40,
						 .order_type = core::OrderType::LIMIT,
						 .timestamp = 2,
						 .participant_id = 1,
				 },
				 "2. Add non-crossing bid");

	submit(matching::InboundOrder{
						 .action = matching::RequestAction::ADD,
						 .symbol = symbol,
						 .order_id = 2002,
						 .side = core::Side::BUY,
						 .price = 1'010'000,
						 .qty = 60,
						 .order_type = core::OrderType::LIMIT,
						 .timestamp = 3,
						 .participant_id = 3,
				 },
				 "3. Add crossing bid");

	submit(matching::InboundOrder{
						 .action = matching::RequestAction::CANCEL,
						 .symbol = symbol,
						 .order_id = 2001,
						 .timestamp = 4,
				 },
				 "4. Cancel remaining bid");
}