-- PostgREST Parser Integration Test Schema
-- This schema demonstrates all relationship types and use cases

-- ============================================================================
-- Core Tables
-- ============================================================================

-- Customers table (parent for orders)
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW()
);

-- Orders table (Many-to-One with customers, One-to-Many with order_items)
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    status VARCHAR(50) DEFAULT 'pending',
    total_amount DECIMAL(10,2) DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Products table (Many-to-Many with orders through order_items)
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    stock INTEGER DEFAULT 0,
    category VARCHAR(100),
    metadata JSONB DEFAULT '{}',
    search_vector TSVECTOR,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Order items junction table (M2M between orders and products)
CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price DECIMAL(10,2) NOT NULL,
    UNIQUE(order_id, product_id)
);

-- ============================================================================
-- Additional tables for M2M relationship testing
-- ============================================================================

-- Posts table
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    author_id INTEGER REFERENCES customers(id),
    published BOOLEAN DEFAULT false,
    published_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Tags table
CREATE TABLE tags (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    color VARCHAR(7) DEFAULT '#000000'
);

-- Post tags junction table (M2M between posts and tags)
CREATE TABLE post_tags (
    post_id INTEGER NOT NULL REFERENCES posts(id),
    tag_id INTEGER NOT NULL REFERENCES tags(id),
    PRIMARY KEY (post_id, tag_id)
);

-- ============================================================================
-- One-to-One relationship example
-- ============================================================================

-- Customer profiles (O2O with customers via unique FK)
CREATE TABLE customer_profiles (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER UNIQUE NOT NULL REFERENCES customers(id),
    bio TEXT,
    avatar_url VARCHAR(500),
    preferences JSONB DEFAULT '{}'
);

-- ============================================================================
-- Full-text search index
-- ============================================================================

CREATE INDEX products_search_idx ON products USING GIN(search_vector);

CREATE OR REPLACE FUNCTION update_product_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := to_tsvector('english', COALESCE(NEW.name, '') || ' ' || COALESCE(NEW.description, ''));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER products_search_update
    BEFORE INSERT OR UPDATE ON products
    FOR EACH ROW
    EXECUTE FUNCTION update_product_search_vector();

-- ============================================================================
-- Seed Data
-- ============================================================================

-- Customers
INSERT INTO customers (name, email, metadata) VALUES
('Alice Johnson', 'alice@example.com', '{"tier": "gold", "preferences": {"newsletter": true}}'),
('Bob Smith', 'bob@example.com', '{"tier": "silver", "preferences": {"newsletter": false}}'),
('Charlie Brown', 'charlie@example.com', '{"tier": "bronze"}'),
('Diana Prince', 'diana@example.com', '{"tier": "gold", "preferences": {"newsletter": true, "notifications": "email"}}'),
('Edward Norton', 'edward@example.com', '{}');

-- Customer profiles (O2O)
INSERT INTO customer_profiles (customer_id, bio, avatar_url, preferences) VALUES
(1, 'Tech enthusiast and early adopter', 'https://example.com/avatars/alice.jpg', '{"theme": "dark"}'),
(2, 'Casual shopper', 'https://example.com/avatars/bob.jpg', '{"theme": "light"}'),
(4, 'Power user since 2020', 'https://example.com/avatars/diana.jpg', '{"theme": "auto"}');

-- Products
INSERT INTO products (name, description, price, stock, category) VALUES
('Laptop Pro', 'High-performance laptop for professionals', 1299.99, 50, 'Electronics'),
('Wireless Mouse', 'Ergonomic wireless mouse with long battery life', 49.99, 200, 'Electronics'),
('Mechanical Keyboard', 'RGB mechanical keyboard with Cherry MX switches', 149.99, 75, 'Electronics'),
('USB-C Hub', 'Multi-port USB-C hub with HDMI and ethernet', 79.99, 150, 'Accessories'),
('Monitor Stand', 'Adjustable monitor stand with USB ports', 129.99, 40, 'Accessories'),
('Webcam HD', '1080p HD webcam with built-in microphone', 89.99, 100, 'Electronics'),
('Desk Lamp', 'LED desk lamp with adjustable brightness', 39.99, 80, 'Office'),
('Notebook Set', 'Premium notebook set with pen', 24.99, 300, 'Office'),
('Coffee Mug', 'Insulated coffee mug with lid', 19.99, 500, 'Kitchen'),
('Water Bottle', 'Stainless steel water bottle 1L', 29.99, 250, 'Kitchen');

-- Orders
INSERT INTO orders (customer_id, status, total_amount, notes) VALUES
(1, 'completed', 1349.98, 'Gift wrap requested'),
(1, 'pending', 149.99, NULL),
(2, 'completed', 79.99, 'Express shipping'),
(2, 'cancelled', 49.99, 'Customer changed mind'),
(3, 'pending', 219.98, NULL),
(4, 'completed', 1479.97, 'Corporate order'),
(4, 'processing', 89.99, NULL);

-- Order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(1, 1, 1, 1299.99),
(1, 2, 1, 49.99),
(2, 3, 1, 149.99),
(3, 4, 1, 79.99),
(4, 2, 1, 49.99),
(5, 3, 1, 149.99),
(5, 7, 2, 39.99),
(6, 1, 1, 1299.99),
(6, 3, 1, 149.99),
(6, 8, 1, 24.99),
(7, 6, 1, 89.99);

-- Tags
INSERT INTO tags (name, color) VALUES
('Technology', '#3498db'),
('Tutorial', '#2ecc71'),
('News', '#e74c3c'),
('Review', '#9b59b6'),
('Tips', '#f1c40f');

-- Posts
INSERT INTO posts (title, content, author_id, published, published_at) VALUES
('Getting Started with PostgreSQL', 'PostgreSQL is a powerful, open source object-relational database...', 1, true, NOW() - INTERVAL '10 days'),
('Top 10 Productivity Apps', 'In this article, we review the best productivity apps of 2024...', 1, true, NOW() - INTERVAL '5 days'),
('Understanding JSON in Postgres', 'JSONB provides powerful JSON handling capabilities...', 2, true, NOW() - INTERVAL '3 days'),
('Draft: Upcoming Features', 'This is a draft post about upcoming features...', 1, false, NULL),
('My Review of the Laptop Pro', 'After using the Laptop Pro for a month, here are my thoughts...', 4, true, NOW() - INTERVAL '1 day');

-- Post tags (M2M)
INSERT INTO post_tags (post_id, tag_id) VALUES
(1, 1), (1, 2),
(2, 1), (2, 5),
(3, 1), (3, 2),
(5, 1), (5, 4);

-- ============================================================================
-- Helpful comments for understanding relationships
-- ============================================================================

COMMENT ON TABLE customers IS 'Customer accounts - parent table for orders and posts';
COMMENT ON TABLE orders IS 'Customer orders - M2O to customers, O2M to order_items';
COMMENT ON TABLE products IS 'Product catalog - M2M to orders through order_items';
COMMENT ON TABLE order_items IS 'Junction table for orders-products M2M relationship';
COMMENT ON TABLE posts IS 'Blog posts - M2O to customers (author), M2M to tags';
COMMENT ON TABLE tags IS 'Post tags - M2M to posts through post_tags';
COMMENT ON TABLE post_tags IS 'Junction table for posts-tags M2M relationship';
COMMENT ON TABLE customer_profiles IS 'O2O extension of customers table';
