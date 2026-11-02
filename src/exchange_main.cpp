#include <iostream>
#include <span>
#include <string_view>

#include "core/types.h"
#include "matching/events.h"
#include "matching/matching_engine.h"
#include "text/order_text.h"

namespace {

using exchange::matching::Event;

void print_result(std::string_view description,
									std::span<const Event> events) {
	std::cout << description << " (" << events.size() << " events)\n";
	for (const Event &event : events) {
		std::cout << "  " << exchange::text::format_event(event) << '\n';
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