package storage

import (
	"context"
	"database/sql"
	"errors"
	"server/internal/model"
	"server/internal/utils"
	"time"

	_ "github.com/lib/pq"
)

type Storage struct {
	config    *Config
	db        *sql.DB
	tasksList []model.Tasks
}

func New(config *Config) *Storage {
	return &Storage{
		config: config,
	}
}

func (s *Storage) Open() error {
	db, err := sql.Open("postgres", s.config.DatabaseURL)
	if err != nil {
		return err
	}

	if err := db.Ping(); err != nil {
		return err
	}

	s.db = db

	return nil
}

func (s *Storage) Close() {
	s.db.Close()
}

func (s *Storage) QueuePush(p *model.Payment) error {
	_, err := s.db.Exec("INSERT INTO queue (account_id, amount, created_at) VALUES($1,$2,$3)",
		p.Account_id, p.Amount, time.Now(),
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *Storage) QueueTasksCount() (int, error) {
	var number_of_tasks int

	ctx := context.Background()

	row := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM queue")
	if err := row.Scan(&number_of_tasks); err != nil {
		return 0, err
	}

	return number_of_tasks, nil
}

func (s *Storage) GetQueueTasks() ([]model.Tasks, error) {

	ctx := context.Background()

	rows, err := s.db.QueryContext(ctx, "SELECT account_id, COUNT(*) FROM queue GROUP BY account_id")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	if err := rows.Err(); err != nil {
		return nil, err
	}

	for rows.Next() {
		var id int
		var count int

		if err := rows.Scan(&id, &count); err != nil {
			return nil, err
		}

		task := model.Tasks{
			ID:    id,
			Count: count,
		}

		s.tasksList = append(s.tasksList, task)
	}

	return s.tasksList, nil

}

func (s *Storage) UpdateBalance(id int) error {
	var balance int
	var queue_id int
	p := &model.Payment{}

	ctx := context.Background()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		tx.Rollback()

		return err
	}

	row := tx.QueryRowContext(
		ctx,
		"SELECT id, account_id, amount FROM (SELECT * FROM queue WHERE account_id=$1 FOR UPDATE) AS a ORDER BY a.id LIMIT 1",
		id,
	)
	if err := row.Scan(&queue_id, &p.Account_id, &p.Amount); err != nil {

		tx.Rollback()

		if err == sql.ErrNoRows {
			return errors.New(utils.ErrNoUpdateBalanceTasks)
		}

		return err
	}

	row = tx.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE id=$1 FOR UPDATE", p.Account_id)
	if err := row.Scan(&balance); err != nil {

		tx.Rollback()

		if err == sql.ErrNoRows {

			return errors.New(utils.ErrNoIdExists)
		}

		return err
	}

	if balance+p.Amount >= 0 {
		_, err := tx.ExecContext(ctx, "UPDATE accounts SET balance=$1 WHERE id=$2", balance+p.Amount, p.Account_id)
		if err != nil {
			tx.Rollback()

			return err
		}

		_, err = tx.ExecContext(
			ctx,
			"INSERT INTO entries (account_id, amount, created_at) VALUES($1,$2,$3)",
			p.Account_id, p.Amount, time.Now(),
		)
		if err != nil {
			tx.Rollback()

			return err
		}

		_, err = tx.ExecContext(ctx, "DELETE FROM queue WHERE id=$1", queue_id)
		if err != nil {
			tx.Rollback()

			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}

		return nil

	} else {

		_, err = tx.ExecContext(ctx, "DELETE FROM queue WHERE id=$1", queue_id)
		if err != nil {
			tx.Rollback()

			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}

		return errors.New(utils.ErrNoMoney)
	}
}
